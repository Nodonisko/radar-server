"""Resolve ready radar products into render jobs and execute them."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence

from .config import GeoBounds, InputConfig, ProductConfig, RenderContext
from .fetching import LocalInputFile
from .input_index import LocalInputIndex
from .rendering.pipeline import Bounds, RenderResult

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class RenderInput:
    input: InputConfig
    files: tuple[LocalInputFile, ...]


@dataclass(frozen=True)
class RenderJob:
    product: ProductConfig
    timestamp: datetime
    inputs: tuple[RenderInput, ...]

    @property
    def files(self) -> tuple[LocalInputFile, ...]:
        return tuple(item for render_input in self.inputs for item in render_input.files)


def resolve_render_jobs(
    input_index: LocalInputIndex,
    products: Iterable[ProductConfig],
    *,
    include_existing: bool = False,
) -> list[RenderJob]:
    """Return render jobs whose product inputs are all available."""

    jobs: list[RenderJob] = []
    for product in products:
        timestamps = sorted(input_index.ready_timestamps(product))
        latest_timestamp = timestamps[-1] if timestamps else None
        for timestamp in timestamps:
            job = _job_for_product_timestamp(input_index, product, timestamp)
            if (
                include_existing
                or not outputs_exist(job)
                or _forecast_outputs_missing(product, timestamp, latest_timestamp, timestamps)
            ):
                jobs.append(job)
    return jobs


def render_ready_jobs(
    input_index: LocalInputIndex,
    products: Iterable[ProductConfig],
    *,
    include_existing: bool = False,
) -> list[RenderResult]:
    products = tuple(products)
    results: list[RenderResult] = []
    latest_by_product = _latest_ready_timestamp_by_product(input_index, products)
    for job in resolve_render_jobs(input_index, products, include_existing=include_existing):
        job_results = render_job(
            job,
            input_index=input_index,
            skip_existing=not include_existing,
            render_forecast=job.timestamp == latest_by_product.get(job.product.id),
        )
        results.extend(job_results)
    return results


def render_job(
    job: RenderJob,
    *,
    input_index: LocalInputIndex | None = None,
    skip_existing: bool = True,
    render_forecast: bool = False,
) -> list[RenderResult]:
    results: list[RenderResult] = []

    if not skip_existing or not outputs_exist(job):
        product = job.product
        render = product.render
        paths = [item.path for item in job.files]
        if not paths:
            raise ValueError(f"render job {product.id!r} has no input files")

        base = output_base(product, job.timestamp)
        bounds = bounds_tuple(product.geo_bounds)
        if len(paths) == 1 and bounds is None:
            res = render.pipeline.render_single(
                paths[0],
                product.output_dir,
                render.palette,
                base=base,
                variants=render.variants,
                optimize=render.optimize,
            )
        else:
            res = render.pipeline.render_composite(
                paths,
                product.output_dir,
                render.palette,
                base=base,
                bounds=bounds,
                variants=render.variants,
                optimize=render.optimize,
            )
        results.append(res)

    if render_forecast and job.product.render.forecast_minutes and input_index is not None:
        forecast_results = _render_forecasts(job, input_index, skip_existing=skip_existing)
        results.extend(forecast_results)

    return results


def outputs_exist(job: RenderJob) -> bool:
    return all(path.exists() for path in expected_output_paths(job.product, job.timestamp))


def expected_output_paths(product: ProductConfig, timestamp: datetime) -> tuple[Path, ...]:
    base = output_base(product, timestamp)
    sidecar = product.output_dir / f"{base}.json"
    variants = tuple(product.output_dir / f"{base}_{name}.png" for name, _ in product.render.variants)
    return (sidecar, *variants)


def expected_forecast_paths(product: ProductConfig, timestamp: datetime) -> tuple[Path, ...]:
    paths: list[Path] = []
    base = output_base(product, timestamp)
    for minute in product.render.forecast_minutes:
        fct_base = f"{base}_fct{minute}"
        sidecar = product.output_dir / "forecast" / f"{fct_base}.json"
        variants = tuple(product.output_dir / "forecast" / f"{fct_base}_{name}.png" for name, _ in product.render.variants)
        paths.extend([sidecar, *variants])
    return tuple(paths)


def _render_forecasts(job: RenderJob, input_index: LocalInputIndex, *, skip_existing: bool) -> list[RenderResult]:
    product = job.product
    if skip_existing and all(path.exists() for path in expected_forecast_paths(product, job.timestamp)):
        LOGGER.debug("Forecast skipped for %s at %s: outputs already exist", product.id, job.timestamp)
        return []

    # Use the previous frame and the current/latest frame for VET motion.
    timestamps = sorted(input_index.ready_timestamps(product))
    try:
        current_idx = timestamps.index(job.timestamp)
    except ValueError:
        LOGGER.info("Forecast skipped for %s at %s: timestamp not found in ready_timestamps", product.id, job.timestamp)
        return []

    if current_idx < 1:
        LOGGER.info("Forecast skipped for %s at %s: no previous frame available for motion tracking", product.id, job.timestamp)
        return []

    past_timestamps = timestamps[current_idx - 1 : current_idx + 1]

    from .rendering.decode import load_odim_hdf
    from .rendering.reproject import to_web_mercator
    from .rendering.composite import composite_to_web_mercator
    from .rendering.forecast import render_forecast

    fields = []
    bounds = bounds_tuple(product.geo_bounds)

    for ts in past_timestamps:
        past_job = _job_for_product_timestamp(input_index, product, ts)
        paths = [item.path for item in past_job.files]
        if not paths:
            LOGGER.info("Forecast skipped for %s at %s: missing input files for past timestamp %s", product.id, job.timestamp, ts)
            return []

        if len(paths) == 1 and bounds is None:
            source_field = load_odim_hdf(paths[0], quantity=product.render.palette.quantity)
            field = to_web_mercator(source_field)
        else:
            source_fields = [load_odim_hdf(p, quantity=product.render.palette.quantity) for p in paths]
            field = composite_to_web_mercator(source_fields, bounds=bounds)
        fields.append(field)

    base = output_base(product, job.timestamp)
    forecast_dir = product.output_dir / "forecast"

    return render_forecast(
        fields=fields,
        output_dir=forecast_dir,
        palette=product.render.palette,
        base=base,
        forecast_minutes=product.render.forecast_minutes,
        method=product.render.forecast_method,
        variants=product.render.variants,
        optimize=product.render.optimize,
    )


def _forecast_outputs_missing(
    product: ProductConfig,
    timestamp: datetime,
    latest_timestamp: datetime | None,
    ready_timestamps: Sequence[datetime],
) -> bool:
    if not product.render.forecast_minutes or timestamp != latest_timestamp:
        return False
    if len(ready_timestamps) < 2:
        return False
    return not all(path.exists() for path in expected_forecast_paths(product, timestamp))


def _latest_ready_timestamp_by_product(
    input_index: LocalInputIndex,
    products: Iterable[ProductConfig],
) -> dict[str, datetime]:
    latest: dict[str, datetime] = {}
    for product in products:
        timestamps = input_index.ready_timestamps(product)
        if timestamps:
            latest[product.id] = max(timestamps)
    return latest


def output_base(product: ProductConfig, timestamp: datetime) -> str:
    return product.base_name(RenderContext(product=product, timestamp=timestamp))


def bounds_tuple(bounds: GeoBounds | None) -> Bounds | None:
    if bounds is None:
        return None
    return (bounds.west, bounds.south, bounds.east, bounds.north)


def _job_for_product_timestamp(
    input_index: LocalInputIndex,
    product: ProductConfig,
    timestamp: datetime,
) -> RenderJob:
    render_inputs = tuple(
        RenderInput(input=input_config, files=input_index.files_for(input_config, timestamp))
        for input_config in product.inputs
    )
    return RenderJob(product=product, timestamp=timestamp, inputs=render_inputs)
