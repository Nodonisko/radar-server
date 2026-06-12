"""Python config-as-code for radar inputs, products, and rendering targets.

This module intentionally links config objects directly instead of by string
references. String IDs still exist for logs, state files, output sidecars, and
cache keys, but config composition should use object references.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Literal, Protocol, Sequence, Union

from .rendering.core import PaletteSpec
from .rendering.palettes import STANDARD_DBZH
from .rendering.pipeline import (
    DEFAULT_VARIANTS,
    Bounds,
    OutputReadyCallback,
    RenderResult,
    render_composite_png,
    render_radar_png,
)


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
ENV_FILE = BASE_DIR.parent / ".env"


def _get_env_value(name: str) -> str | None:
    value = os.environ.get(name)
    if value is not None:
        return value
    if not ENV_FILE.exists():
        return None

    for line in ENV_FILE.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        if key.strip() != name:
            continue
        return raw_value.strip().strip("\"'")
    return None


def timestamp_from_yyyymmddhhmmss(value: str) -> datetime | None:
    """Extract the first YYYYMMDDhhmmss timestamp from a filename or URL."""

    match = re.search(r"(\d{8})(\d{6})", value)
    if match is None:
        return None
    return datetime.strptime("".join(match.groups()), "%Y%m%d%H%M%S")


def timestamp_from_ord_name(value: str) -> datetime | None:
    """Extract timestamps from ORD names such as OPERA@20260604T0220@0@DBZH.h5."""

    match = re.search(r"(\d{8})T(\d{4})(\d{2})?", value)
    if match is None:
        return timestamp_from_yyyymmddhhmmss(value)
    date_part, hour_minute, seconds = match.groups()
    return datetime.strptime(f"{date_part}{hour_minute}{seconds or '00'}", "%Y%m%d%H%M%S")


@dataclass(frozen=True)
class GeoBounds:
    """Lon/lat bounds in EPSG:4326: west, south, east, north."""

    west: float
    south: float
    east: float
    north: float


@dataclass(frozen=True)
class SmartPollingPolicy:
    """Polling policy compatible with the old quick-polling scheduler behavior."""

    expected_period_seconds: int = 300
    baseline_interval_seconds: int = 300
    quick_check_interval_seconds: int = 3
    quick_check_limit: int = 90


@dataclass(frozen=True)
class NotificationPolicy:
    kind: Literal["mqtt"]
    host: str
    port: int
    username: str | None = None
    password: str | None = None
    topic: str | None = None
    path: str | None = None
    tls: bool = True


@dataclass(frozen=True)
class RetentionPolicy:
    keep_for_seconds: int | None = 7200


@dataclass(frozen=True)
class InputAvailabilityPolicy:
    """How the input layer should revisit missing expected timestamps.

    This mirrors the old scheduler's backlog behavior: missing inputs stay in
    the tracked window and are retried. Products only observe whether their
    configured inputs are available.
    """

    retry_interval_seconds: int = 30
    warn_after_seconds: int = 3600


VariantSpec = tuple[str, float]


class SingleFileRenderer(Protocol):
    def __call__(
        self,
        hdf_path: Path,
        output_dir: Path,
        palette: PaletteSpec,
        *,
        base: str,
        variants: Sequence[VariantSpec] = DEFAULT_VARIANTS,
        optimize: bool = True,
        on_output_ready: OutputReadyCallback | None = None,
    ) -> RenderResult: ...


class CompositeRenderer(Protocol):
    def __call__(
        self,
        hdf_paths: Iterable[Path],
        output_dir: Path,
        palette: PaletteSpec,
        *,
        base: str,
        bounds: Bounds | None = None,
        variants: Sequence[VariantSpec] = DEFAULT_VARIANTS,
        optimize: bool = True,
        on_output_ready: OutputReadyCallback | None = None,
    ) -> RenderResult: ...


@dataclass(frozen=True)
class RenderPipeline:
    id: str
    render_single: SingleFileRenderer
    render_composite: CompositeRenderer


DEFAULT_RENDER_PIPELINE = RenderPipeline(
    id="default_png",
    render_single=render_radar_png,
    render_composite=render_composite_png,
)


@dataclass(frozen=True)
class RenderProfile:
    pipeline: RenderPipeline = DEFAULT_RENDER_PIPELINE
    palette: PaletteSpec = STANDARD_DBZH
    variants: tuple[VariantSpec, ...] = DEFAULT_VARIANTS
    optimize: bool = True


@dataclass(frozen=True)
class HttpDirectorySource:
    id: str
    label: str
    base_url: str
    polling: SmartPollingPolicy


@dataclass(frozen=True)
class OrdApiSource:
    id: str
    label: str
    api_base_url: str = "https://api.meteogate.eu/eu-eumetnet-weather-radar"
    s3_endpoint_url: str = "https://s3.waw3-1.cloudferro.com"
    rolling_bucket: str = "openradar-24h"
    api_key_env: str | None = "METEOGATE_API_KEY"
    api_key_header: str = "apikey"
    polling: SmartPollingPolicy = SmartPollingPolicy()
    notifications: tuple[NotificationPolicy, ...] = (
        NotificationPolicy(
            kind="mqtt",
            host="radar.meteogate.eu",
            port=8884,
            username="everyone",
            password="everyone",
            topic="ORD/eu.eumetnet/0-20010-0-OPERA/DBZH",
            path="/ordmqtt",
            tls=True,
        ),
    )

    def api_key(self) -> str | None:
        if self.api_key_env is None:
            return None
        return _get_env_value(self.api_key_env)


SourceConfig = Union[HttpDirectorySource, OrdApiSource]


@dataclass(frozen=True)
class OrdItemsQuery:
    """ORD API discovery query for collections/observations/items.

    The downloader can later turn matching response features into concrete ODIM
    download URLs using each feature's ``properties.data`` link. ORD's actual
    HTTP parameters are hyphenated, e.g. ``standard-name`` and
    ``naming-authority``.
    """

    bbox: GeoBounds
    standard_name: str = "DBZH"
    fmt: str = "ODIM"
    method: str = "scan"
    naming_authority: str | None = None
    platform_code_prefixes: tuple[str, ...] = ()
    lookback_minutes: int = 30
    notes: str = ""


@dataclass(frozen=True)
class OrdLocationQuery:
    """ORD API query for collections/observations/locations/{location_id}."""

    location_id: str
    standard_name: str = "DBZH"
    fmt: str = "ODIM"
    method: Literal["comp", "scan"] = "comp"
    lookback_minutes: int = 30
    notes: str = ""


@dataclass(frozen=True)
class InputConfig:
    id: str
    label: str
    source: SourceConfig
    local_dir: Path
    file_suffixes: tuple[str, ...] = (".h5", ".hdf")
    quantity: str = "DBZH"
    odim_product: str | None = None
    timestamp_from_name: Callable[[str], datetime | None] = timestamp_from_yyyymmddhhmmss
    remote_query: OrdItemsQuery | OrdLocationQuery | None = None
    availability: InputAvailabilityPolicy = InputAvailabilityPolicy()
    retention: RetentionPolicy = RetentionPolicy()
    status: Literal["ready", "provisional"] = "ready"
    enabled: bool = True


@dataclass(frozen=True)
class RenderContext:
    product: ProductConfig
    timestamp: datetime


BaseNameFactory = Callable[[RenderContext], str]


@dataclass(frozen=True)
class ProductConfig:
    """A configured render target.

    Products with multiple inputs match files by exact timestamp. European radar
    products are expected to publish on the same standardized five-minute grid.

    ``priority`` orders render jobs: lower numbers render first.
    """

    id: str
    label: str
    inputs: tuple[InputConfig, ...]
    output_dir: Path
    geo_bounds: GeoBounds
    base_name: BaseNameFactory
    render: RenderProfile = RenderProfile()
    priority: int = 100
    warn_if_pending_after_seconds: int = 3600
    retention: RetentionPolicy = RetentionPolicy()
    enabled: bool = True


@dataclass(frozen=True)
class ForecastProduct:
    """A derived nowcast product linked to a parent observed product.

    Forecast generation consumes the parent's last ``history_frames`` input
    frames, extrapolates them ``minutes`` ahead, and stores the computed fields
    on disk (under ``field_dir``) so forecast frame rendering behaves like an
    ordinary product render. Geo bounds, palette, and (by default) variants are
    reused from the parent; ``priority`` should be a high number so forecast
    work never preempts observed renders.
    """

    id: str
    parent: ProductConfig
    minutes: tuple[int, ...] = (10, 20, 30, 40, 50, 60)
    method: str = "lucaskanade"
    history_frames: int = 3
    priority: int = 1000
    variants: tuple[VariantSpec, ...] | None = None
    field_dir: Path | None = None
    enabled: bool = True

    def __post_init__(self) -> None:
        if self.field_dir is None:
            object.__setattr__(self, "field_dir", DATA_DIR / self.parent.id / "forecast_fields")

    @property
    def palette(self) -> PaletteSpec:
        return self.parent.render.palette

    @property
    def geo_bounds(self) -> GeoBounds:
        return self.parent.geo_bounds

    @property
    def output_dir(self) -> Path:
        return self.parent.output_dir.parent / "forecast" / self.parent.id

    @property
    def render_variants(self) -> tuple[VariantSpec, ...]:
        return self.variants if self.variants is not None else self.parent.render.variants

    @property
    def optimize(self) -> bool:
        return self.parent.render.optimize

    @property
    def retention(self) -> RetentionPolicy:
        return self.parent.retention


def inputs(*items: InputConfig) -> tuple[InputConfig, ...]:
    """Tiny helper so product config reads like inputs(cz, de)."""

    return items


def timestamped_base(prefix: str) -> BaseNameFactory:
    def base_name(ctx: RenderContext) -> str:
        return f"{prefix}_{ctx.timestamp:%Y%m%d_%H%M}"

    return base_name


CHMI_SMART_POLLING = SmartPollingPolicy(
    expected_period_seconds=300,
    baseline_interval_seconds=300,
    quick_check_interval_seconds=3,
    quick_check_limit=90,
)

ORD_SMART_POLLING = SmartPollingPolicy(
    expected_period_seconds=300,
    baseline_interval_seconds=300,
    quick_check_interval_seconds=15,
    quick_check_limit=30,
)


chmi_current = HttpDirectorySource(
    id="chmi_current",
    label="CHMI current MAX-Z composites",
    base_url="https://opendata.chmi.cz/meteorology/weather/radar/composite/maxz/hdf5/",
    polling=CHMI_SMART_POLLING,
)

ord_api = OrdApiSource(
    id="ord_api",
    label="EUMETNET Open Radar Data API",
    polling=ORD_SMART_POLLING,
)


CZECHIA_BOUNDS = GeoBounds(west=11.266869, south=48.047275, east=19.623974, north=51.458369)
GERMANY_BOUNDS = GeoBounds(west=5.5, south=47.2, east=15.2, north=55.2)
POLAND_BOUNDS = GeoBounds(west=14.1, south=49.0, east=24.2, north=54.9)
SLOVAKIA_BOUNDS = GeoBounds(west=16.8, south=47.7, east=22.6, north=49.7)
AUSTRIA_BOUNDS = GeoBounds(west=9.4, south=46.3, east=17.2, north=49.1)
CENTRAL_EUROPE_BOUNDS = GeoBounds(west=5.5, south=46.0, east=24.2, north=55.2)


cz_maxz = InputConfig(
    id="cz_maxz",
    label="Czechia CHMI MAX-Z composite",
    source=chmi_current,
    local_dir=DATA_DIR / "cz" / "chmi_maxz",
    quantity="DBZH",
    odim_product="MAX",
)

opera_dbzh = InputConfig(
    id="opera_dbzh",
    label="EUMETNET OPERA DBZH composite",
    source=ord_api,
    local_dir=DATA_DIR / "opera" / "dbzh",
    timestamp_from_name=timestamp_from_ord_name,
    remote_query=OrdLocationQuery(
        location_id="0-20010-0-OPERA",
        method="comp",
        notes="Use this as the reliable ORD-backed input for countries without confirmed national feeds.",
    ),
)

cz_product = ProductConfig(
    id="cz",
    label="Czechia radar",
    inputs=inputs(cz_maxz),
    output_dir=OUTPUT_DIR / "cz",
    geo_bounds=CZECHIA_BOUNDS,
    base_name=timestamped_base("radar_cz"),
    priority=0,
)

de_product = ProductConfig(
    id="de",
    label="Germany radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "de",
    geo_bounds=GERMANY_BOUNDS,
    base_name=timestamped_base("radar_de"),
    priority=10,
)

pl_product = ProductConfig(
    id="pl",
    label="Poland radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "pl",
    geo_bounds=POLAND_BOUNDS,
    base_name=timestamped_base("radar_pl"),
    priority=10,
)

sk_product = ProductConfig(
    id="sk",
    label="Slovakia radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "sk",
    geo_bounds=SLOVAKIA_BOUNDS,
    base_name=timestamped_base("radar_sk"),
    priority=10,
)

at_product = ProductConfig(
    id="at",
    label="Austria radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "at",
    geo_bounds=AUSTRIA_BOUNDS,
    base_name=timestamped_base("radar_at"),
    priority=10,
)

central_europe_product = ProductConfig(
    id="central_europe",
    label="Central Europe OPERA composite",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "central_europe",
    geo_bounds=CENTRAL_EUROPE_BOUNDS,
    base_name=timestamped_base("radar_central_europe"),
    priority=20,
)


cz_forecast = ForecastProduct(id="cz_forecast", parent=cz_product, priority=1000)
de_forecast = ForecastProduct(id="de_forecast", parent=de_product, priority=1010)
pl_forecast = ForecastProduct(id="pl_forecast", parent=pl_product, priority=1010)
sk_forecast = ForecastProduct(id="sk_forecast", parent=sk_product, priority=1010)
at_forecast = ForecastProduct(id="at_forecast", parent=at_product, priority=1010)
central_europe_forecast = ForecastProduct(
    id="central_europe_forecast",
    parent=central_europe_product,
    priority=1020,
)


SOURCES: tuple[SourceConfig, ...] = (chmi_current, ord_api)
INPUTS: tuple[InputConfig, ...] = (cz_maxz, opera_dbzh)
COUNTRY_PRODUCTS: tuple[ProductConfig, ...] = (
    cz_product,
    de_product,
    pl_product,
    sk_product,
    at_product,
)
COMPOSITE_PRODUCTS: tuple[ProductConfig, ...] = (
    central_europe_product,
)
PRODUCTS: tuple[ProductConfig, ...] = COUNTRY_PRODUCTS + COMPOSITE_PRODUCTS
FORECAST_PRODUCTS: tuple[ForecastProduct, ...] = (
    cz_forecast,
    de_forecast,
    pl_forecast,
    sk_forecast,
    at_forecast,
    central_europe_forecast,
)


@dataclass(frozen=True)
class RadarServerConfig:
    sources: tuple[SourceConfig, ...] = SOURCES
    inputs: tuple[InputConfig, ...] = INPUTS
    products: tuple[ProductConfig, ...] = PRODUCTS
    forecasts: tuple[ForecastProduct, ...] = FORECAST_PRODUCTS


CONFIG = RadarServerConfig()
