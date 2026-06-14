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


CZECHIA_BOUNDS = GeoBounds(west=11.27, south=48.05, east=19.62, north=51.46)
GERMANY_BOUNDS = GeoBounds(west=4.71, south=46.55, east=16.17, north=55.62)
POLAND_BOUNDS = GeoBounds(west=12.94, south=48.27, east=25.28, north=55.56)
SLOVAKIA_BOUNDS = GeoBounds(west=15.76, south=47.03, east=23.63, north=50.32)
AUSTRIA_BOUNDS = GeoBounds(west=8.46, south=45.66, east=18.22, north=49.73)
BELGIUM_BOUNDS = GeoBounds(west=1.38, south=48.78, east=7.50, north=52.22)
SWITZERLAND_BOUNDS = GeoBounds(west=4.92, south=45.10, east=11.51, north=48.52)
DENMARK_BOUNDS = GeoBounds(west=6.82, south=54.08, east=12.26, north=58.47)
ESTONIA_BOUNDS = GeoBounds(west=22.02, south=56.80, east=29.59, north=60.39)
FINLAND_BOUNDS = GeoBounds(west=18.62, south=59.09, east=33.14, north=70.79)
FRANCE_BOUNDS = GeoBounds(west=-5.87, south=41.61, east=9.29, north=51.81)
GREECE_BOUNDS = GeoBounds(west=19.07, south=35.67, east=27.59, north=42.47)
CROATIA_BOUNDS = GeoBounds(west=12.48, south=42.22, east=20.43, north=47.27)
HUNGARY_BOUNDS = GeoBounds(west=15.05, south=45.02, east=23.95, north=49.29)
IRELAND_BOUNDS = GeoBounds(west=-11.65, south=50.73, east=-4.80, north=56.10)
ICELAND_BOUNDS = GeoBounds(west=-26.27, south=62.68, east=-11.80, north=67.25)
ITALY_BOUNDS = GeoBounds(west=5.59, south=37.20, east=19.46, north=47.81)
LATVIA_BOUNDS = GeoBounds(west=19.67, south=54.95, east=29.51, north=58.79)
LITHUANIA_BOUNDS = GeoBounds(west=19.77, south=53.17, east=28.06, north=57.16)
MOLDOVA_BOUNDS = GeoBounds(west=25.54, south=44.74, east=31.17, north=49.21)
MALTA_BOUNDS = GeoBounds(west=13.44, south=35.08, east=15.45, north=36.71)
NETHERLANDS_BOUNDS = GeoBounds(west=2.29, south=50.03, east=8.40, north=54.18)
NORWAY_BOUNDS = GeoBounds(west=3.45, south=57.26, east=33.19, north=71.85)
PORTUGAL_BOUNDS = GeoBounds(west=-10.42, south=36.27, east=-5.25, north=42.87)
ROMANIA_BOUNDS = GeoBounds(west=19.21, south=42.93, east=30.72, north=48.99)
SERBIA_BOUNDS = GeoBounds(west=17.82, south=41.52, east=23.97, north=46.89)
SPAIN_BOUNDS = GeoBounds(west=-10.27, south=35.29, east=4.29, north=44.51)
SWEDEN_BOUNDS = GeoBounds(west=9.72, south=54.62, east=25.91, north=69.75)
SLOVENIA_BOUNDS = GeoBounds(west=12.33, south=44.70, east=17.56, north=47.58)
UNITED_KINGDOM_BOUNDS = GeoBounds(west=-7.54, south=49.24, east=2.95, north=59.40)
CENTRAL_EUROPE_BOUNDS = GeoBounds(west=5.50, south=46.00, east=24.20, north=55.20)


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
    priority=5,
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

be_product = ProductConfig(
    id="be",
    label="Belgium radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "be",
    geo_bounds=BELGIUM_BOUNDS,
    base_name=timestamped_base("radar_be"),
    priority=10,
)

ch_product = ProductConfig(
    id="ch",
    label="Switzerland radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "ch",
    geo_bounds=SWITZERLAND_BOUNDS,
    base_name=timestamped_base("radar_ch"),
    priority=10,
)

dk_product = ProductConfig(
    id="dk",
    label="Denmark radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "dk",
    geo_bounds=DENMARK_BOUNDS,
    base_name=timestamped_base("radar_dk"),
    priority=10,
)

ee_product = ProductConfig(
    id="ee",
    label="Estonia radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "ee",
    geo_bounds=ESTONIA_BOUNDS,
    base_name=timestamped_base("radar_ee"),
    priority=10,
)

fi_product = ProductConfig(
    id="fi",
    label="Finland radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "fi",
    geo_bounds=FINLAND_BOUNDS,
    base_name=timestamped_base("radar_fi"),
    priority=10,
)

fr_product = ProductConfig(
    id="fr",
    label="France radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "fr",
    geo_bounds=FRANCE_BOUNDS,
    base_name=timestamped_base("radar_fr"),
    priority=10,
)

gb_product = ProductConfig(
    id="gb",
    label="United Kingdom radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "gb",
    geo_bounds=UNITED_KINGDOM_BOUNDS,
    base_name=timestamped_base("radar_gb"),
    priority=10,
)

gr_product = ProductConfig(
    id="gr",
    label="Greece radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "gr",
    geo_bounds=GREECE_BOUNDS,
    base_name=timestamped_base("radar_gr"),
    priority=10,
)

hr_product = ProductConfig(
    id="hr",
    label="Croatia radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "hr",
    geo_bounds=CROATIA_BOUNDS,
    base_name=timestamped_base("radar_hr"),
    priority=10,
)

hu_product = ProductConfig(
    id="hu",
    label="Hungary radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "hu",
    geo_bounds=HUNGARY_BOUNDS,
    base_name=timestamped_base("radar_hu"),
    priority=10,
)

ie_product = ProductConfig(
    id="ie",
    label="Ireland radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "ie",
    geo_bounds=IRELAND_BOUNDS,
    base_name=timestamped_base("radar_ie"),
    priority=10,
)

is_product = ProductConfig(
    id="is",
    label="Iceland radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "is",
    geo_bounds=ICELAND_BOUNDS,
    base_name=timestamped_base("radar_is"),
    priority=10,
)

it_product = ProductConfig(
    id="it",
    label="Italy radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "it",
    geo_bounds=ITALY_BOUNDS,
    base_name=timestamped_base("radar_it"),
    priority=10,
)

lv_product = ProductConfig(
    id="lv",
    label="Latvia radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "lv",
    geo_bounds=LATVIA_BOUNDS,
    base_name=timestamped_base("radar_lv"),
    priority=10,
)

lt_product = ProductConfig(
    id="lt",
    label="Lithuania radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "lt",
    geo_bounds=LITHUANIA_BOUNDS,
    base_name=timestamped_base("radar_lt"),
    priority=10,
)

md_product = ProductConfig(
    id="md",
    label="Moldova radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "md",
    geo_bounds=MOLDOVA_BOUNDS,
    base_name=timestamped_base("radar_md"),
    priority=10,
)

mt_product = ProductConfig(
    id="mt",
    label="Malta radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "mt",
    geo_bounds=MALTA_BOUNDS,
    base_name=timestamped_base("radar_mt"),
    priority=10,
)

nl_product = ProductConfig(
    id="nl",
    label="Netherlands radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "nl",
    geo_bounds=NETHERLANDS_BOUNDS,
    base_name=timestamped_base("radar_nl"),
    priority=10,
)

no_product = ProductConfig(
    id="no",
    label="Norway radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "no",
    geo_bounds=NORWAY_BOUNDS,
    base_name=timestamped_base("radar_no"),
    priority=10,
)

pt_product = ProductConfig(
    id="pt",
    label="Portugal radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "pt",
    geo_bounds=PORTUGAL_BOUNDS,
    base_name=timestamped_base("radar_pt"),
    priority=10,
)

ro_product = ProductConfig(
    id="ro",
    label="Romania radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "ro",
    geo_bounds=ROMANIA_BOUNDS,
    base_name=timestamped_base("radar_ro"),
    priority=10,
)

rs_product = ProductConfig(
    id="rs",
    label="Serbia radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "rs",
    geo_bounds=SERBIA_BOUNDS,
    base_name=timestamped_base("radar_rs"),
    priority=10,
)

es_product = ProductConfig(
    id="es",
    label="Spain radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "es",
    geo_bounds=SPAIN_BOUNDS,
    base_name=timestamped_base("radar_es"),
    priority=10,
)

se_product = ProductConfig(
    id="se",
    label="Sweden radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "se",
    geo_bounds=SWEDEN_BOUNDS,
    base_name=timestamped_base("radar_se"),
    priority=10,
)

si_product = ProductConfig(
    id="si",
    label="Slovenia radar",
    inputs=inputs(opera_dbzh),
    output_dir=OUTPUT_DIR / "si",
    geo_bounds=SLOVENIA_BOUNDS,
    base_name=timestamped_base("radar_si"),
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
be_forecast = ForecastProduct(id="be_forecast", parent=be_product, priority=1010)
ch_forecast = ForecastProduct(id="ch_forecast", parent=ch_product, priority=1010)
dk_forecast = ForecastProduct(id="dk_forecast", parent=dk_product, priority=1010)
ee_forecast = ForecastProduct(id="ee_forecast", parent=ee_product, priority=1010)
fi_forecast = ForecastProduct(id="fi_forecast", parent=fi_product, priority=1010)
fr_forecast = ForecastProduct(id="fr_forecast", parent=fr_product, priority=1010)
gb_forecast = ForecastProduct(id="gb_forecast", parent=gb_product, priority=1010)
gr_forecast = ForecastProduct(id="gr_forecast", parent=gr_product, priority=1010)
hr_forecast = ForecastProduct(id="hr_forecast", parent=hr_product, priority=1010)
hu_forecast = ForecastProduct(id="hu_forecast", parent=hu_product, priority=1010)
ie_forecast = ForecastProduct(id="ie_forecast", parent=ie_product, priority=1010)
is_forecast = ForecastProduct(id="is_forecast", parent=is_product, priority=1010)
it_forecast = ForecastProduct(id="it_forecast", parent=it_product, priority=1010)
lv_forecast = ForecastProduct(id="lv_forecast", parent=lv_product, priority=1010)
lt_forecast = ForecastProduct(id="lt_forecast", parent=lt_product, priority=1010)
md_forecast = ForecastProduct(id="md_forecast", parent=md_product, priority=1010)
mt_forecast = ForecastProduct(id="mt_forecast", parent=mt_product, priority=1010)
nl_forecast = ForecastProduct(id="nl_forecast", parent=nl_product, priority=1010)
no_forecast = ForecastProduct(id="no_forecast", parent=no_product, priority=1010)
pt_forecast = ForecastProduct(id="pt_forecast", parent=pt_product, priority=1010)
ro_forecast = ForecastProduct(id="ro_forecast", parent=ro_product, priority=1010)
rs_forecast = ForecastProduct(id="rs_forecast", parent=rs_product, priority=1010)
es_forecast = ForecastProduct(id="es_forecast", parent=es_product, priority=1010)
se_forecast = ForecastProduct(id="se_forecast", parent=se_product, priority=1010)
si_forecast = ForecastProduct(id="si_forecast", parent=si_product, priority=1010)
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
    be_product,
    ch_product,
    dk_product,
    ee_product,
    fi_product,
    fr_product,
    gb_product,
    gr_product,
    hr_product,
    hu_product,
    ie_product,
    is_product,
    it_product,
    lv_product,
    lt_product,
    md_product,
    mt_product,
    nl_product,
    no_product,
    pt_product,
    ro_product,
    rs_product,
    es_product,
    se_product,
    si_product,
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
    be_forecast,
    ch_forecast,
    dk_forecast,
    ee_forecast,
    fi_forecast,
    fr_forecast,
    gb_forecast,
    gr_forecast,
    hr_forecast,
    hu_forecast,
    ie_forecast,
    is_forecast,
    it_forecast,
    lv_forecast,
    lt_forecast,
    md_forecast,
    mt_forecast,
    nl_forecast,
    no_forecast,
    pt_forecast,
    ro_forecast,
    rs_forecast,
    es_forecast,
    se_forecast,
    si_forecast,
    central_europe_forecast,
)


@dataclass(frozen=True)
class RadarServerConfig:
    sources: tuple[SourceConfig, ...] = SOURCES
    inputs: tuple[InputConfig, ...] = INPUTS
    products: tuple[ProductConfig, ...] = PRODUCTS
    forecasts: tuple[ForecastProduct, ...] = FORECAST_PRODUCTS


CONFIG = RadarServerConfig()
