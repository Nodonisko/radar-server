from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from radar_server.config import (
    CONFIG,
    DATA_DIR,
    FORECAST_PRODUCTS,
    PRODUCTS,
    ForecastProduct,
    cz_forecast,
    cz_product,
)
from radar_server.__main__ import _with_optimize


def test_forecast_product_reuses_parent_settings() -> None:
    assert cz_forecast.parent is cz_product
    assert cz_forecast.palette is cz_product.render.palette
    assert cz_forecast.geo_bounds is cz_product.geo_bounds
    assert cz_forecast.render_variants == cz_product.render.variants
    assert cz_forecast.optimize == cz_product.render.optimize
    assert cz_forecast.retention is cz_product.retention
    assert cz_forecast.output_dir == cz_product.output_dir.parent / "forecast" / "cz"
    assert cz_forecast.field_dir == DATA_DIR / "cz" / "forecast_fields"


def test_forecast_product_variant_override(tmp_path: Path) -> None:
    forecast = ForecastProduct(
        id="custom",
        parent=cz_product,
        variants=(("overlay", 2.0),),
        field_dir=tmp_path / "fields",
    )

    assert forecast.render_variants == (("overlay", 2.0),)
    assert forecast.field_dir == tmp_path / "fields"


def test_priorities_put_czechia_first_and_forecasts_last() -> None:
    priorities = {product.id: product.priority for product in PRODUCTS}
    assert priorities["cz"] == 0
    assert priorities["cz"] < min(p for pid, p in priorities.items() if pid != "cz")

    max_observed = max(priorities.values())
    assert all(forecast.priority > max_observed for forecast in FORECAST_PRODUCTS)
    forecast_priorities = {forecast.id: forecast.priority for forecast in FORECAST_PRODUCTS}
    assert forecast_priorities["cz_forecast"] == min(forecast_priorities.values())


def test_config_wires_every_product_to_a_forecast() -> None:
    assert CONFIG.forecasts == FORECAST_PRODUCTS
    parent_ids = {forecast.parent.id for forecast in CONFIG.forecasts}
    assert parent_ids == {product.id for product in CONFIG.products}
    assert all(forecast.history_frames == 3 for forecast in FORECAST_PRODUCTS)
    assert all(forecast.method == "lucaskanade" for forecast in FORECAST_PRODUCTS)


def test_oxipng_disabled_for_selected_country_products() -> None:
    disabled_products = {product.id for product in PRODUCTS if not product.render.optimize}
    assert disabled_products == {"fi", "no", "se"}

    disabled_forecasts = {forecast.id for forecast in FORECAST_PRODUCTS if not forecast.optimize}
    assert disabled_forecasts == {"fi_forecast", "no_forecast", "se_forecast"}


def test_cli_optimize_default_preserves_product_settings() -> None:
    assert _with_optimize(CONFIG, optimize=True) is CONFIG

    config = _with_optimize(CONFIG, optimize=False)

    assert all(not product.render.optimize for product in config.products)
    assert all(not forecast.optimize for forecast in config.forecasts)


def test_configured_product_bounds_use_at_most_two_decimals() -> None:
    for product in PRODUCTS:
        bounds = product.geo_bounds
        values = (bounds.west, bounds.south, bounds.east, bounds.north)
        assert all(-Decimal(str(value)).as_tuple().exponent <= 2 for value in values)
