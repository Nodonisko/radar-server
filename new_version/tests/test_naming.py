from datetime import datetime

from new_version.naming import background_filename, overlay_filename, extract_forecast_timestamp


def test_background_filename():
    ts = datetime(2025, 9, 26, 20, 20)
    assert (
        background_filename(ts)
        == "background_radar_20250926_2020_300.png"
    )
    assert (
        background_filename(ts, forecast=True, offset=5)
        == "background_radar_20250926_2020_forecast_fct05_300.png"
    )


def test_overlay_filename():
    ts = datetime(2025, 9, 26, 20, 20)
    assert (
        overlay_filename(ts, "overlay")
        == "radar_20250926_2020_overlay.png"
    )
    assert (
        overlay_filename(ts, "overlay2x", forecast=True, offset=10)
        == "radar_20250926_2020_forecast_fct10_overlay2x.png"
    )


def test_extract_forecast_timestamp():
    # Test with correct forecast TAR filename
    filename = "T_PABV23_C_OKPR_20250928.2225.ft60s10.tar"
    expected = datetime(2025, 9, 28, 22, 25)
    result = extract_forecast_timestamp(filename)
    assert result == expected

    # Test with different time
    filename = "T_PABV23_C_OKPR_20250928.1830.ft60s10.tar"
    expected = datetime(2025, 9, 28, 18, 30)
    result = extract_forecast_timestamp(filename)
    assert result == expected

    # Test with invalid filename
    filename = "invalid_filename.tar"
    result = extract_forecast_timestamp(filename)
    assert result is None


