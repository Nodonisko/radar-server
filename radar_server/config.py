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
GERMANY_BOUNDS = GeoBounds(west=4.711586, south=46.551379, east=16.168036, north=55.624271)
POLAND_BOUNDS = GeoBounds(west=12.936388, south=48.274700, east=25.279047, north=55.557258)
SLOVAKIA_BOUNDS = GeoBounds(west=15.764866, south=47.031193, east=23.634744, north=50.321029)
AUSTRIA_BOUNDS = GeoBounds(west=8.463484, south=45.658871, east=18.220469, north=49.728704)
BELGIUM_BOUNDS = GeoBounds(west=1.380375, south=48.776326, east=7.497718, north=52.215314)
SWITZERLAND_BOUNDS = GeoBounds(west=4.918130, south=45.100918, east=11.514118, north=48.520097)
DENMARK_BOUNDS = GeoBounds(west=6.819675, south=54.082861, east=12.261313, north=58.469516)
ESTONIA_BOUNDS = GeoBounds(west=22.015533, south=56.797783, east=29.592520, north=60.388974)
FINLAND_BOUNDS = GeoBounds(west=18.621891, south=59.093060, east=33.143316, north=70.793045)
FRANCE_BOUNDS = GeoBounds(west=-5.865889, south=41.605182, east=9.292734, north=51.807256)
GREECE_BOUNDS = GeoBounds(west=19.067082, south=35.669837, east=27.593434, north=42.470556)
CROATIA_BOUNDS = GeoBounds(west=12.479137, south=42.221883, east=20.426300, north=47.266748)
HUNGARY_BOUNDS = GeoBounds(west=15.045243, south=45.021469, east=23.948594, north=49.288577)
IRELAND_BOUNDS = GeoBounds(west=-11.647136, south=50.726517, east=-4.803301, north=56.104912)
ICELAND_BOUNDS = GeoBounds(west=-26.267531, south=62.679498, east=-11.801860, north=67.254000)
ITALY_BOUNDS = GeoBounds(west=5.585726, south=37.197847, east=19.456396, north=47.805419)
LATVIA_BOUNDS = GeoBounds(west=19.674605, south=54.948480, east=29.508835, north=58.793388)
LITHUANIA_BOUNDS = GeoBounds(west=19.765159, south=53.168020, east=28.059620, north=57.160886)
MOLDOVA_BOUNDS = GeoBounds(west=25.541452, south=44.741855, east=31.172302, north=49.205247)
MALTA_BOUNDS = GeoBounds(west=13.435455, south=35.080185, east=15.452499, north=36.714591)
NETHERLANDS_BOUNDS = GeoBounds(west=2.294029, south=50.028691, east=8.395435, north=54.178726)
NORWAY_BOUNDS = GeoBounds(west=3.447983, south=57.260988, east=33.193923, north=71.845183)
PORTUGAL_BOUNDS = GeoBounds(west=-10.418533, south=36.265721, east=-5.247149, north=42.873803)
ROMANIA_BOUNDS = GeoBounds(west=19.207807, south=42.930296, east=30.717722, north=48.993792)
SERBIA_BOUNDS = GeoBounds(west=17.815915, south=41.515407, east=23.968314, north=46.892730)
SPAIN_BOUNDS = GeoBounds(west=-10.272448, south=35.285097, east=4.289206, north=44.513467)
SWEDEN_BOUNDS = GeoBounds(west=9.718319, south=54.623097, east=25.914358, north=69.754606)
SLOVENIA_BOUNDS = GeoBounds(west=12.326989, south=44.704546, east=17.557504, north=47.583470)
UNITED_KINGDOM_BOUNDS = GeoBounds(west=-7.543125, south=49.239511, east=2.949023, north=59.395834)
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
