from __future__ import annotations

import argparse
import json
import re
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyproj import CRS, Transformer
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely.ops import transform


NATURAL_EARTH_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/"
    "ne_10m_admin_0_countries.geojson"
)
DEFAULT_BUFFER_KM = 80.0
CONFIG_PATH = Path("radar_server/config.py")


@dataclass(frozen=True)
class CountryBoundsSpec:
    constant: str
    iso_a2: str | None = None
    admin: str | None = None


COUNTRIES: dict[str, CountryBoundsSpec] = {
    "at": CountryBoundsSpec("AUSTRIA_BOUNDS", iso_a2="AT"),
    "be": CountryBoundsSpec("BELGIUM_BOUNDS", iso_a2="BE"),
    "ch": CountryBoundsSpec("SWITZERLAND_BOUNDS", iso_a2="CH"),
    "de": CountryBoundsSpec("GERMANY_BOUNDS", iso_a2="DE"),
    "dk": CountryBoundsSpec("DENMARK_BOUNDS", iso_a2="DK"),
    "ee": CountryBoundsSpec("ESTONIA_BOUNDS", iso_a2="EE"),
    "es": CountryBoundsSpec("SPAIN_BOUNDS", iso_a2="ES"),
    "fi": CountryBoundsSpec("FINLAND_BOUNDS", iso_a2="FI"),
    "fr": CountryBoundsSpec("FRANCE_BOUNDS", iso_a2="FR"),
    "gb": CountryBoundsSpec("UNITED_KINGDOM_BOUNDS", iso_a2="GB"),
    "gr": CountryBoundsSpec("GREECE_BOUNDS", iso_a2="GR"),
    "hr": CountryBoundsSpec("CROATIA_BOUNDS", iso_a2="HR"),
    "hu": CountryBoundsSpec("HUNGARY_BOUNDS", iso_a2="HU"),
    "ie": CountryBoundsSpec("IRELAND_BOUNDS", iso_a2="IE"),
    "is": CountryBoundsSpec("ICELAND_BOUNDS", iso_a2="IS"),
    "it": CountryBoundsSpec("ITALY_BOUNDS", iso_a2="IT"),
    "lt": CountryBoundsSpec("LITHUANIA_BOUNDS", iso_a2="LT"),
    "lv": CountryBoundsSpec("LATVIA_BOUNDS", iso_a2="LV"),
    "md": CountryBoundsSpec("MOLDOVA_BOUNDS", iso_a2="MD", admin="Moldova"),
    "mt": CountryBoundsSpec("MALTA_BOUNDS", iso_a2="MT"),
    "nl": CountryBoundsSpec("NETHERLANDS_BOUNDS", iso_a2="NL"),
    "no": CountryBoundsSpec("NORWAY_BOUNDS", iso_a2="NO"),
    "pl": CountryBoundsSpec("POLAND_BOUNDS", iso_a2="PL"),
    "pt": CountryBoundsSpec("PORTUGAL_BOUNDS", iso_a2="PT"),
    "ro": CountryBoundsSpec("ROMANIA_BOUNDS", iso_a2="RO"),
    "rs": CountryBoundsSpec("SERBIA_BOUNDS", iso_a2="RS"),
    "se": CountryBoundsSpec("SWEDEN_BOUNDS", iso_a2="SE"),
    "si": CountryBoundsSpec("SLOVENIA_BOUNDS", iso_a2="SI"),
    "sk": CountryBoundsSpec("SLOVAKIA_BOUNDS", iso_a2="SK"),
}


def _load_features(url: str) -> list[dict[str, Any]]:
    with urllib.request.urlopen(url, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload["features"]


def _matches_country(feature: dict[str, Any], spec: CountryBoundsSpec) -> bool:
    props = feature["properties"]
    if spec.iso_a2 and props.get("ISO_A2") == spec.iso_a2:
        return True
    if spec.iso_a2 and props.get("ISO_A2_EH") == spec.iso_a2:
        return True
    if spec.admin and props.get("ADMIN") == spec.admin:
        return True
    return False


def _local_laea_crs(geometry: Polygon | MultiPolygon) -> CRS:
    lon, lat = geometry.representative_point().coords[0]
    return CRS.from_proj4(f"+proj=laea +lat_0={lat} +lon_0={lon} +datum=WGS84 +units=m +no_defs")


def _largest_projected_polygon(geometry: Polygon | MultiPolygon, crs: CRS) -> Polygon:
    to_local = Transformer.from_crs("EPSG:4326", crs, always_xy=True).transform
    if isinstance(geometry, Polygon):
        return transform(to_local, geometry)

    polygons = [transform(to_local, polygon) for polygon in geometry.geoms]
    return max(polygons, key=lambda polygon: polygon.area)


def _buffered_mainland_bounds(feature: dict[str, Any], *, buffer_km: float) -> tuple[float, float, float, float]:
    geometry = shape(feature["geometry"])
    crs = _local_laea_crs(geometry)
    mainland = _largest_projected_polygon(geometry, crs)
    buffered = mainland.buffer(buffer_km * 1000.0)
    to_wgs84 = Transformer.from_crs(crs, "EPSG:4326", always_xy=True).transform
    west, south, east, north = transform(to_wgs84, buffered).bounds
    return west, south, east, north


def _format_bounds(bounds: tuple[float, float, float, float]) -> str:
    west, south, east, north = bounds
    return (
        f"GeoBounds(west={west:.6f}, south={south:.6f}, "
        f"east={east:.6f}, north={north:.6f})"
    )


def generate_bounds(*, buffer_km: float, url: str = NATURAL_EARTH_URL) -> dict[str, tuple[float, float, float, float]]:
    features = _load_features(url)
    bounds: dict[str, tuple[float, float, float, float]] = {}
    for country_id, spec in COUNTRIES.items():
        match = next((feature for feature in features if _matches_country(feature, spec)), None)
        if match is None:
            raise RuntimeError(f"Natural Earth feature not found for {country_id} ({spec})")
        bounds[country_id] = _buffered_mainland_bounds(match, buffer_km=buffer_km)
    return bounds


def update_config(bounds_by_country: dict[str, tuple[float, float, float, float]], config_path: Path) -> None:
    config = config_path.read_text()
    for country_id, bounds in bounds_by_country.items():
        constant = COUNTRIES[country_id].constant
        replacement = f"{constant} = {_format_bounds(bounds)}"
        config, count = re.subn(rf"^{constant} = GeoBounds\([^)]+\)", replacement, config, count=1, flags=re.MULTILINE)
        if count != 1:
            raise RuntimeError(f"Expected one {constant} assignment in {config_path}")
    config_path.write_text(config)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate mainland country bounds with a projected buffer.")
    parser.add_argument("--buffer-km", type=float, default=DEFAULT_BUFFER_KM)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--check", action="store_true", help="Print bounds without modifying config.py.")
    args = parser.parse_args()

    bounds = generate_bounds(buffer_km=args.buffer_km)
    if args.check:
        for country_id in sorted(bounds):
            print(f"{COUNTRIES[country_id].constant} = {_format_bounds(bounds[country_id])}")
        return

    update_config(bounds, args.config)


if __name__ == "__main__":
    main()
