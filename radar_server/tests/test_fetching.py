from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace

import numpy as np
from numcodecs import Blosc

import radar_server.fetching as fetching
from radar_server.config import RetentionPolicy, cz_maxz, it_sri_arco
from radar_server.fetching import (
    RemoteInputFile,
    SkippableFetchError,
    _discover_arco,
    _download_arco_frame,
    download_remote_file,
    sync_input,
)
from radar_server.rendering.decode import load_odim_hdf


_FAKE_GEOREF = {
    "projdef": "+proj=longlat +datum=WGS84 +no_defs",
    "xsize": 3,
    "ysize": 2,
    "xscale": 0.1,
    "yscale": 0.1,
    "UL_lon": 10.0,
    "UL_lat": 45.0,
    "dtype": "<f4",
    "variable": "RR",
}


def _fake_metadata(last_valid: str) -> dict:
    return {
        ".zattrs": {"last_valid": last_valid},
        "crs/.zattrs": {
            "proj4": _FAKE_GEOREF["projdef"],
            "where_xsize": _FAKE_GEOREF["xsize"],
            "where_ysize": _FAKE_GEOREF["ysize"],
            "where_xscale": _FAKE_GEOREF["xscale"],
            "where_yscale": _FAKE_GEOREF["yscale"],
            "where_UL_lon": _FAKE_GEOREF["UL_lon"],
            "where_UL_lat": _FAKE_GEOREF["UL_lat"],
        },
        "RR/.zarray": {"dtype": _FAKE_GEOREF["dtype"]},
    }


def test_discover_arco_enumerates_frontier_backwards(monkeypatch, tmp_path) -> None:
    input_config = replace(it_sri_arco, local_dir=tmp_path)
    axis = np.array(
        ["2026-06-19T17:50", "2026-06-19T17:55", "2026-06-19T18:00"], dtype="datetime64[m]"
    )
    monkeypatch.setattr(fetching, "_arco_auth", lambda source: ("user", "key"))
    monkeypatch.setattr(fetching, "_arco_metadata", lambda source, auth: _fake_metadata("2026-06-19T18:00"))
    monkeypatch.setattr(fetching, "_arco_time_axis", lambda source, auth: axis)

    files = _discover_arco(input_config, input_config.source, now=None, limit=2)

    assert [f.timestamp for f in files] == [datetime(2026, 6, 19, 18, 0), datetime(2026, 6, 19, 17, 55)]
    assert files[0].url.endswith("/RR/2.0.0")
    assert files[1].url.endswith("/RR/1.0.0")
    assert files[0].filename == "it_sri_arco_20260619180000.h5"
    assert files[0].metadata["arco_index"] == 2


def test_download_arco_frame_writes_decodable_odim(monkeypatch, tmp_path) -> None:
    values = np.array([[0.0, 2.5, np.nan], [np.nan, 10.0, 0.0]], dtype="<f4")
    encoded = Blosc().encode(values.tobytes())
    monkeypatch.setattr(fetching, "_arco_auth", lambda source: ("user", "key"))
    monkeypatch.setattr(fetching, "_arco_get", lambda url, auth: SimpleNamespace(content=encoded))

    input_config = replace(it_sri_arco, local_dir=tmp_path)
    remote = RemoteInputFile(
        input=input_config,
        timestamp=datetime(2026, 6, 19, 18, 0),
        url="https://example.test/radar.zarr/RR/2.0.0",
        filename="it_sri_arco_20260619180000.h5",
        metadata={"arco_index": 2, "arco_georef": _FAKE_GEOREF, "arco_dtype": "<f4"},
    )

    local = _download_arco_frame(remote, tmp_path / remote.filename)

    assert local.downloaded is True
    field = load_odim_hdf(local.path, quantity="RATE")
    assert field.quantity == "RATE"
    assert field.transform.width == 3 and field.transform.height == 2
    decoded = field.values
    assert np.isnan(decoded[0, 2]) and np.isnan(decoded[1, 0])  # NaN round-trips as nodata
    assert decoded[0, 1] == 2.5 and decoded[1, 1] == 10.0  # wet cells preserved
    assert decoded[0, 0] == 0.0 and decoded[1, 2] == 0.0  # dry cells stay 0 (render transparent)


def test_sync_input_skips_missing_arco_chunk(monkeypatch, tmp_path) -> None:
    input_config = replace(it_sri_arco, local_dir=tmp_path, retention=RetentionPolicy(keep_for_seconds=7200))
    now = datetime(2026, 6, 19, 18, 1)
    remote = RemoteInputFile(
        input=input_config,
        timestamp=datetime(2026, 6, 19, 18, 0),
        url="https://example.test/radar.zarr/RR/2.0.0",
        filename="it_sri_arco_20260619180000.h5",
        metadata={"arco_index": 2, "arco_georef": _FAKE_GEOREF, "arco_dtype": "<f4"},
    )
    monkeypatch.setattr(fetching, "discover_remote_files", lambda *a, **k: [remote])

    def boom(_remote):
        raise SkippableFetchError("chunk not available")

    monkeypatch.setattr(fetching, "download_remote_file", boom)

    assert sync_input(input_config, now=now) == []


def test_download_remote_file_dispatches_arco(monkeypatch, tmp_path) -> None:
    values = np.zeros((2, 3), dtype="<f4")
    encoded = Blosc().encode(values.tobytes())
    monkeypatch.setattr(fetching, "_arco_auth", lambda source: ("user", "key"))
    monkeypatch.setattr(fetching, "_arco_get", lambda url, auth: SimpleNamespace(content=encoded))
    input_config = replace(it_sri_arco, local_dir=tmp_path)
    remote = RemoteInputFile(
        input=input_config,
        timestamp=datetime(2026, 6, 19, 18, 0),
        url="https://example.test/radar.zarr/RR/2.0.0",
        filename="it_sri_arco_20260619180000.h5",
        metadata={"arco_index": 2, "arco_georef": _FAKE_GEOREF, "arco_dtype": "<f4"},
    )

    local = download_remote_file(remote)
    assert local.path.exists() and local.downloaded is True


def test_sync_input_filters_discovered_files_outside_retention(monkeypatch, tmp_path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path, retention=RetentionPolicy(keep_for_seconds=7200))
    now = datetime(2026, 6, 5, 21, 5)
    old = RemoteInputFile(
        input=input_config,
        timestamp=datetime(2026, 6, 5, 18, 55),
        url="https://example.test/old.hdf",
        filename="old.hdf",
        metadata={},
    )
    recent = RemoteInputFile(
        input=input_config,
        timestamp=datetime(2026, 6, 5, 19, 5),
        url="https://example.test/recent.hdf",
        filename="recent.hdf",
        metadata={},
    )
    downloaded = []

    monkeypatch.setattr("radar_server.fetching.discover_remote_files", lambda *args, **kwargs: [old, recent])

    def fake_download(remote):
        downloaded.append(remote)
        return remote

    monkeypatch.setattr("radar_server.fetching.download_remote_file", fake_download)

    sync_input(input_config, now=now)

    assert downloaded == [recent]
