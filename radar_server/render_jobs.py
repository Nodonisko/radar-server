"""Resolve ready radar products into render jobs and execute them.

This module is forecast-free: forecast generation and rendering live in
``forecast_generation``/``forecast_store`` and run as separate queue tasks.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from .config import GeoBounds, InputConfig, ProductConfig, RenderContext
from .fetching import LocalInputFile
from .input_index import LocalInputIndex
from .rendering.layered import is_layered_pipeline
from .rendering.pipeline import Bounds, OutputReadyCallback, RenderResult

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
        for timestamp in sorted(input_index.ready_timestamps(product)):
            job = job_for_product_timestamp(input_index, product, timestamp)
            if include_existing or not outputs_exist(job):
                jobs.append(job)
    return jobs


def render_ready_jobs(
    input_index: LocalInputIndex,
    products: Iterable[ProductConfig],
    *,
    include_existing: bool = False,
    on_output_ready: OutputReadyCallback | None = None,
) -> list[RenderResult]:
    results: list[RenderResult] = []
    for job in resolve_render_jobs(input_index, tuple(products), include_existing=include_existing):
        result = render_job(job, skip_existing=not include_existing, on_output_ready=on_output_ready)
        if result is not None:
            results.append(result)
    return results


def render_job(
    job: RenderJob,
    *,
    skip_existing: bool = True,
    on_output_ready: OutputReadyCallback | None = None,
) -> RenderResult | None:
    if skip_existing and outputs_exist(job):
        return None

    product = job.product
    render = product.render
    paths = _job_paths(job)
    if not paths:
        raise ValueError(f"render job {product.id!r} has no input files")

    base = output_base(product, job.timestamp)
    bounds = bounds_tuple(product.geo_bounds)
    if len(paths) == 1 and bounds is None and render.pipeline.render_single is not None:
        return render.pipeline.render_single(
            paths[0],
            product.output_dir,
            render.palette,
            base=base,
            variants=render.variants,
            optimize=render.optimize,
            nodata_fill=render.nodata_fill,
            on_output_ready=on_output_ready,
        )
    return render.pipeline.render_composite(
        paths,
        product.output_dir,
        render.palette,
        base=base,
        bounds=bounds,
        variants=render.variants,
        optimize=render.optimize,
        nodata_fill=render.nodata_fill,
        on_output_ready=on_output_ready,
    )


def _job_paths(job: RenderJob) -> list[Path]:
    """Input paths for one render, normalized for the product's pipeline.

    Layered pipelines require exactly one file per input in input order, so a
    re-published duplicate for one timestamp must not become a third input;
    the newest file (by name) per input wins. Other pipelines composite every
    available file.
    """
    if is_layered_pipeline(job.product.render.pipeline.id):
        return [
            max(render_input.files, key=lambda item: item.path.name).path
            for render_input in job.inputs
            if render_input.files
        ]
    return [item.path for item in job.files]


def outputs_exist(job: RenderJob) -> bool:
    return all(path.exists() for path in expected_output_paths(job.product, job.timestamp))


def expected_output_paths(product: ProductConfig, timestamp: datetime) -> tuple[Path, ...]:
    base = output_base(product, timestamp)
    sidecar = product.output_dir / f"{base}.json"
    variants = tuple(product.output_dir / f"{base}_{name}.png" for name, _ in product.render.variants)
    return (sidecar, *variants)


def output_base(product: ProductConfig, timestamp: datetime) -> str:
    return product.base_name(RenderContext(product=product, timestamp=timestamp))


def bounds_tuple(bounds: GeoBounds | None) -> Bounds | None:
    if bounds is None:
        return None
    return (bounds.west, bounds.south, bounds.east, bounds.north)


def job_for_product_timestamp(
    input_index: LocalInputIndex,
    product: ProductConfig,
    timestamp: datetime,
) -> RenderJob:
    render_inputs = tuple(
        RenderInput(input=input_config, files=input_index.files_for(input_config, timestamp))
        for input_config in product.inputs
    )
    return RenderJob(product=product, timestamp=timestamp, inputs=render_inputs)
