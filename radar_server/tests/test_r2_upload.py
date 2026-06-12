from __future__ import annotations

import json
from pathlib import Path

import pytest

from radar_server import config as config_module
from radar_server.r2_upload import (
    DEFAULT_CACHE_CONTROL,
    R2UploadConfig,
    R2UploadState,
    R2UploadWorker,
    R2Uploader,
)


class FakeS3Client:
    def __init__(self) -> None:
        self.uploads = []

    def upload_fileobj(self, file_obj, bucket, key, ExtraArgs=None):  # noqa: ANN001, ANN002, N803
        self.uploads.append(
            {
                "body": file_obj.read(),
                "bucket": bucket,
                "key": key,
                "extra": ExtraArgs,
            }
        )


class FlakyS3Client(FakeS3Client):
    def __init__(self) -> None:
        super().__init__()
        self.failures_remaining = 1

    def upload_fileobj(self, file_obj, bucket, key, ExtraArgs=None):  # noqa: ANN001, ANN002, N803
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise RuntimeError("temporary R2 outage")
        super().upload_fileobj(file_obj, bucket, key, ExtraArgs=ExtraArgs)


def _config(*, prefix: str = "") -> R2UploadConfig:
    return R2UploadConfig(
        enabled=True,
        account_id="account",
        bucket="radar",
        access_key_id="access",
        secret_access_key="secret",
        prefix=prefix,
        cache_control="public, max-age=60",
    )


def test_r2_uploader_mirrors_output_tree_and_skips_sidecars(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    png = output_dir / "cz" / "radar_cz_20260605_2105_overlay.png"
    sidecar = output_dir / "cz" / "radar_cz_20260605_2105.json"
    png.parent.mkdir(parents=True)
    png.write_bytes(b"png")
    sidecar.write_text("{}")
    client = FakeS3Client()
    uploader = R2Uploader(_config(prefix="radar-prod/"), output_dir=output_dir, client=client)

    assert uploader.upload_path(sidecar) is None
    assert uploader.upload_path(png) == "radar-prod/cz/radar_cz_20260605_2105_overlay.png"

    assert client.uploads == [
        {
            "body": b"png",
            "bucket": "radar",
            "key": "radar-prod/cz/radar_cz_20260605_2105_overlay.png",
            "extra": {
                "ContentType": "image/png",
                "CacheControl": "public, max-age=60",
            },
        }
    ]


def test_r2_upload_worker_queues_pngs_only(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    png = output_dir / "cz" / "frame_overlay.png"
    sidecar = output_dir / "cz" / "frame.json"
    png.parent.mkdir(parents=True)
    png.write_bytes(b"png")
    sidecar.write_text("{}")
    client = FakeS3Client()
    worker = R2UploadWorker(R2Uploader(_config(), output_dir=output_dir, client=client))

    worker.enqueue(sidecar)
    worker.enqueue(png)

    assert worker.process_one() is True
    assert worker.process_one() is False
    assert client.uploads[0]["key"] == "cz/frame_overlay.png"


def test_r2_upload_worker_retries_failures_and_marks_success(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    png = output_dir / "cz" / "frame_overlay.png"
    png.parent.mkdir(parents=True)
    png.write_bytes(b"png")
    client = FlakyS3Client()
    state = R2UploadState(output_dir)
    worker = R2UploadWorker(
        R2Uploader(_config(), output_dir=output_dir, client=client),
        retry_delay_seconds=0,
        state=state,
    )

    worker.enqueue(png)

    assert worker.process_one() is True
    assert worker.pending_count() == 1
    assert client.uploads == []
    assert worker.process_one() is True

    assert worker.pending_count() == 0
    assert client.uploads[0]["key"] == "cz/frame_overlay.png"
    payload = json.loads((output_dir / ".r2_upload_state.json").read_text())
    assert payload["uploaded"]["cz/frame_overlay.png"]["size"] == 3


def test_r2_upload_reconciliation_skips_marked_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    uploaded = output_dir / "cz" / "uploaded_overlay.png"
    missing = output_dir / "cz" / "missing_overlay.png"
    uploaded.parent.mkdir(parents=True)
    uploaded.write_bytes(b"uploaded")
    missing.write_bytes(b"missing")
    state = R2UploadState(output_dir)
    state.mark_uploaded(uploaded)
    client = FakeS3Client()
    worker = R2UploadWorker(R2Uploader(_config(), output_dir=output_dir, client=client), state=state)

    assert worker.enqueue_pending_outputs() == 1
    assert worker.process_one() is True

    assert client.uploads[0]["key"] == "cz/missing_overlay.png"


def test_r2_env_bool_rejects_typos(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(config_module, "ENV_FILE", tmp_path / ".env")
    monkeypatch.setenv("RADAR_R2_ENABLED", "Y")

    with pytest.raises(ValueError, match="RADAR_R2_ENABLED"):
        R2UploadConfig.from_env()


def test_empty_env_value_overrides_dotenv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("RADAR_R2_ENABLED=1\n")
    monkeypatch.setattr(config_module, "ENV_FILE", dotenv)
    monkeypatch.setenv("RADAR_R2_ENABLED", "")

    assert R2UploadConfig.from_env().enabled is False


def test_r2_cache_control_is_fixed_not_env_configured(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "\n".join(
            [
                "RADAR_R2_ENABLED=1",
                "RADAR_R2_ACCOUNT_ID=account",
                "RADAR_R2_BUCKET=radar",
                "RADAR_R2_ACCESS_KEY_ID=access",
                "RADAR_R2_SECRET_ACCESS_KEY=secret",
                "RADAR_R2_CACHE_CONTROL=public, max-age=60",
            ]
        )
    )
    monkeypatch.setattr(config_module, "ENV_FILE", dotenv)

    assert R2UploadConfig.from_env().cache_control == DEFAULT_CACHE_CONTROL
