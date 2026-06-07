from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path

from radar_server.config import GeoBounds, ProductConfig, RetentionPolicy, cz_maxz, timestamped_base
from radar_server.pruning import prune_input_files, prune_product_outputs


def _product(tmp_path: Path, *, retention: RetentionPolicy | None = None) -> ProductConfig:
    return ProductConfig(
        id="test",
        label="Test",
        inputs=(cz_maxz,),
        output_dir=tmp_path,
        geo_bounds=GeoBounds(0, 0, 0, 0),
        base_name=timestamped_base("radar_test"),
        retention=retention or RetentionPolicy(),
    )


def test_prune_input_files_deletes_old_matching_files_only(tmp_path: Path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path, retention=RetentionPolicy(keep_for_seconds=7200))
    old_file = tmp_path / "T_PABV23_C_OKPR_20260605180500.hdf"
    recent_file = tmp_path / "T_PABV23_C_OKPR_20260605190500.hdf"
    part_file = tmp_path / "T_PABV23_C_OKPR_20260605180500.hdf.part"
    unknown_file = tmp_path / "unknown.hdf"
    wrong_suffix = tmp_path / "T_PABV23_C_OKPR_20260605180500.txt"
    for path in (old_file, recent_file, part_file, unknown_file, wrong_suffix):
        path.write_bytes(b"x")

    result = prune_input_files([input_config], now=datetime(2026, 6, 5, 21, 5))

    assert result.deleted == (old_file,)
    assert not old_file.exists()
    assert recent_file.exists()
    assert part_file.exists()
    assert unknown_file.exists()
    assert wrong_suffix.exists()


def test_prune_input_files_respects_disabled_retention(tmp_path: Path) -> None:
    input_config = replace(cz_maxz, local_dir=tmp_path, retention=RetentionPolicy(keep_for_seconds=None))
    old_file = tmp_path / "T_PABV23_C_OKPR_20260605180500.hdf"
    old_file.write_bytes(b"x")

    result = prune_input_files([input_config], now=datetime(2026, 6, 5, 21, 5))

    assert result.deleted == ()
    assert old_file.exists()


def test_prune_product_outputs_deletes_old_frame_group(tmp_path: Path) -> None:
    product = _product(tmp_path, retention=RetentionPolicy(keep_for_seconds=7200))
    old_sidecar = tmp_path / "radar_test_20260605_1805.json"
    old_overlay = tmp_path / "radar_test_20260605_1805_overlay.png"
    old_small = tmp_path / "radar_test_20260605_1805_overlay_small.png"
    recent_sidecar = tmp_path / "radar_test_20260605_1905.json"
    recent_overlay = tmp_path / "radar_test_20260605_1905_overlay.png"
    unknown_sidecar = tmp_path / "other_20260605_1805.json"
    for path in (old_sidecar, old_overlay, old_small, recent_sidecar, recent_overlay, unknown_sidecar):
        path.write_bytes(b"x")

    result = prune_product_outputs([product], now=datetime(2026, 6, 5, 21, 5))

    assert result.deleted == (old_sidecar, old_overlay, old_small)
    assert not old_sidecar.exists()
    assert not old_overlay.exists()
    assert not old_small.exists()
    assert recent_sidecar.exists()
    assert recent_overlay.exists()
    assert unknown_sidecar.exists()


def test_prune_product_outputs_handles_missing_variant(tmp_path: Path) -> None:
    product = _product(tmp_path, retention=RetentionPolicy(keep_for_seconds=7200))
    old_sidecar = tmp_path / "radar_test_20260605_1805.json"
    old_overlay = tmp_path / "radar_test_20260605_1805_overlay.png"
    old_sidecar.write_bytes(b"x")
    old_overlay.write_bytes(b"x")

    result = prune_product_outputs([product], now=datetime(2026, 6, 5, 21, 5))

    assert result.deleted == (old_sidecar, old_overlay)
    assert not old_sidecar.exists()
    assert not old_overlay.exists()
