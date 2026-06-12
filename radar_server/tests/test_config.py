from __future__ import annotations

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
    assert priorities["central_europe"] == max(priorities.values())

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
