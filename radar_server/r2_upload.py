"""Background uploads of rendered PNG outputs to Cloudflare R2."""

from __future__ import annotations

import json
import logging
import mimetypes
import queue
import threading
import time
from dataclasses import dataclass, field
from itertools import count
from pathlib import Path
from typing import Protocol

from .config import OUTPUT_DIR, _get_env_value

LOGGER = logging.getLogger(__name__)

DEFAULT_CACHE_CONTROL = "public, max-age=31536000, immutable"
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5
DEFAULT_READ_TIMEOUT_SECONDS = 20
DEFAULT_RETRY_DELAY_SECONDS = 5.0
MAX_RETRY_DELAY_SECONDS = 300.0
UPLOAD_STATE_FILENAME = ".r2_upload_state.json"
PNG_SUFFIX = ".png"


class S3Client(Protocol):
    def upload_fileobj(self, Fileobj, Bucket: str, Key: str, ExtraArgs: dict | None = None) -> None: ...  # noqa: ANN001


@dataclass(frozen=True)
class R2UploadConfig:
    enabled: bool = False
    account_id: str | None = None
    bucket: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    prefix: str = ""
    cache_control: str = DEFAULT_CACHE_CONTROL

    @classmethod
    def from_env(cls) -> "R2UploadConfig":
        enabled = _env_bool("RADAR_R2_ENABLED")
        if not enabled:
            return cls(enabled=False)

        config = cls(
            enabled=True,
            account_id=_get_env_value("RADAR_R2_ACCOUNT_ID"),
            bucket=_get_env_value("RADAR_R2_BUCKET"),
            access_key_id=_get_env_value("RADAR_R2_ACCESS_KEY_ID"),
            secret_access_key=_get_env_value("RADAR_R2_SECRET_ACCESS_KEY"),
            prefix=_get_env_value("RADAR_R2_PREFIX") or "",
            cache_control=DEFAULT_CACHE_CONTROL,
        )
        missing = (
            name
            for name, value in (
                ("RADAR_R2_ACCOUNT_ID", config.account_id),
                ("RADAR_R2_BUCKET", config.bucket),
                ("RADAR_R2_ACCESS_KEY_ID", config.access_key_id),
                ("RADAR_R2_SECRET_ACCESS_KEY", config.secret_access_key),
            )
            if not value
        )
        missing_names = tuple(missing)
        if missing_names:
            raise ValueError(f"R2 upload is enabled but missing env vars: {', '.join(missing_names)}")
        return config

    @property
    def endpoint_url(self) -> str:
        if not self.account_id:
            raise ValueError("R2 account_id is required when upload is enabled")
        return f"https://{self.account_id}.r2.cloudflarestorage.com"


def _env_bool(name: str) -> bool:
    value = _get_env_value(name)
    if value is None:
        return False
    normalized = value.strip().lower()
    if normalized == "":
        return False
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be one of 1/0, true/false, yes/no, or on/off")


class R2Uploader:
    def __init__(
        self,
        config: R2UploadConfig,
        *,
        output_dir: Path = OUTPUT_DIR,
        client: S3Client | None = None,
    ) -> None:
        self.config = config
        self.output_dir = output_dir
        self._client = client

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def upload_path(self, path: Path) -> str | None:
        """Upload one rendered PNG and return the R2 key, or None when skipped."""

        if not self.enabled or path.suffix.lower() != PNG_SUFFIX:
            return None
        if not path.exists():
            LOGGER.info("Skipping R2 upload for missing local output %s", path)
            return None

        key = self.key_for(path)
        extra_args = {
            "ContentType": mimetypes.guess_type(path.name)[0] or "application/octet-stream",
            "CacheControl": self.config.cache_control,
        }
        with path.open("rb") as file_obj:
            self._s3().upload_fileobj(file_obj, self._bucket(), key, ExtraArgs=extra_args)
        LOGGER.info("Uploaded %s to R2 as %s", path.name, key)
        return key

    def validate(self) -> None:
        """Fail fast for missing dependencies or incomplete enabled config."""

        if not self.enabled:
            return
        self._bucket()
        self._s3()

    def key_for(self, path: Path) -> str:
        relative = path.resolve().relative_to(self.output_dir.resolve()).as_posix()
        prefix = self.config.prefix.strip("/")
        return f"{prefix}/{relative}" if prefix else relative

    def _bucket(self) -> str:
        if not self.config.bucket:
            raise ValueError("R2 bucket is required when upload is enabled")
        return self.config.bucket

    def _s3(self) -> S3Client:
        if self._client is None:
            self._client = _build_s3_client(self.config)
        return self._client


def _build_s3_client(config: R2UploadConfig) -> S3Client:
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise RuntimeError("boto3 is required for R2 uploads; install requirements.txt") from exc

    return boto3.client(
        service_name="s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        region_name="auto",
        config=Config(
            connect_timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
            read_timeout=DEFAULT_READ_TIMEOUT_SECONDS,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


@dataclass(order=True)
class _QueuedUpload:
    ready_at: float
    sequence: int
    path: Path = field(compare=False)
    attempts: int = field(default=0, compare=False)


class R2UploadState:
    """Persistent record of PNGs successfully uploaded to R2."""

    def __init__(self, output_dir: Path = OUTPUT_DIR, *, state_path: Path | None = None) -> None:
        self.output_dir = output_dir
        self.state_path = state_path or output_dir / UPLOAD_STATE_FILENAME
        self._lock = threading.Lock()
        self._records = self._load()

    def is_uploaded(self, path: Path) -> bool:
        stat = _safe_stat(path)
        if stat is None:
            return False
        key = self._relative_key(path)
        with self._lock:
            record = self._records.get(key)
        if record is None:
            return False
        return record.get("size") == stat.st_size and record.get("mtime_ns") == stat.st_mtime_ns

    def mark_uploaded(self, path: Path) -> None:
        stat = _safe_stat(path)
        if stat is None:
            return
        key = self._relative_key(path)
        with self._lock:
            self._records[key] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
            self._write_locked()

    def prune_missing(self, existing_paths: set[Path]) -> None:
        existing_keys = {self._relative_key(path) for path in existing_paths}
        with self._lock:
            stale = [key for key in self._records if key not in existing_keys]
            if not stale:
                return
            for key in stale:
                del self._records[key]
            self._write_locked()

    def _relative_key(self, path: Path) -> str:
        return path.resolve().relative_to(self.output_dir.resolve()).as_posix()

    def _load(self) -> dict[str, dict[str, int]]:
        try:
            payload = json.loads(self.state_path.read_text())
        except FileNotFoundError:
            return {}
        except Exception:
            LOGGER.warning("Ignoring unreadable R2 upload state %s", self.state_path)
            return {}
        if not isinstance(payload, dict):
            return {}
        records = payload.get("uploaded", {})
        return records if isinstance(records, dict) else {}

    def _write_locked(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.state_path.with_name(f"{self.state_path.name}.part")
        tmp_path.write_text(json.dumps({"uploaded": self._records}, indent=2, sort_keys=True))
        tmp_path.replace(self.state_path)


class R2UploadWorker(threading.Thread):
    def __init__(
        self,
        uploader: R2Uploader,
        *,
        poll_interval: float = 0.5,
        retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
        max_retry_delay_seconds: float = MAX_RETRY_DELAY_SECONDS,
        state: R2UploadState | None = None,
    ) -> None:
        super().__init__(name="r2-upload-worker", daemon=True)
        self.uploader = uploader
        self._queue: queue.PriorityQueue[_QueuedUpload] = queue.PriorityQueue()
        self._poll_interval = poll_interval
        self._retry_delay_seconds = retry_delay_seconds
        self._max_retry_delay_seconds = max_retry_delay_seconds
        self._state = state if state is not None else (R2UploadState(uploader.output_dir) if uploader.enabled else None)
        self._stop_event = threading.Event()
        self._pending: set[Path] = set()
        self._pending_lock = threading.Lock()
        self._sequence = count()

    @classmethod
    def from_env(cls, *, output_dir: Path = OUTPUT_DIR) -> "R2UploadWorker":
        return cls(R2Uploader(R2UploadConfig.from_env(), output_dir=output_dir))

    @classmethod
    def disabled(cls) -> "R2UploadWorker":
        return cls(R2Uploader(R2UploadConfig(enabled=False)))

    @property
    def enabled(self) -> bool:
        return self.uploader.enabled

    def validate(self) -> None:
        self.uploader.validate()

    def enqueue(self, path: Path) -> None:
        self._enqueue_path(path)

    def enqueue_pending_outputs(self) -> int:
        """Queue local PNGs that have no matching successful-upload record."""

        if not self.enabled:
            return 0
        if not self.uploader.output_dir.exists():
            return 0

        existing = {path for path in self.uploader.output_dir.rglob(f"*{PNG_SUFFIX}") if path.is_file()}
        if self._state is not None:
            self._state.prune_missing(existing)

        enqueued = 0
        for output_path in sorted(existing):
            if self._state is not None and self._state.is_uploaded(output_path):
                continue
            if self._enqueue_path(output_path):
                enqueued += 1
        if enqueued:
            LOGGER.info("Queued %d local PNGs for R2 upload reconciliation", enqueued)
        return enqueued

    def run(self) -> None:
        while not self._stop_event.is_set() or not self._queue.empty():
            self.process_one(timeout=self._poll_interval)

    def process_one(self, *, timeout: float = 0.0) -> bool:
        try:
            task = self._queue.get(timeout=timeout)
        except queue.Empty:
            return False
        now = time.monotonic()
        if task.ready_at > now:
            if self._stop_event.is_set():
                self._mark_not_pending(task.path)
                self._queue.task_done()
                return False
            self._queue.put(task)
            self._queue.task_done()
            time.sleep(min(task.ready_at - now, self._poll_interval))
            return False
        try:
            key = self.uploader.upload_path(task.path)
            if key is not None and self._state is not None:
                self._state.mark_uploaded(task.path)
            self._mark_not_pending(task.path)
        except Exception:
            if self._stop_event.is_set():
                LOGGER.exception("R2 upload failed for %s; will retry on next start", task.path)
                self._mark_not_pending(task.path)
            else:
                delay = self._retry_delay(task.attempts + 1)
                LOGGER.exception(
                    "R2 upload failed for %s; retrying in %.0fs (attempt %d)",
                    task.path,
                    delay,
                    task.attempts + 1,
                )
                self._queue.put(
                    _QueuedUpload(
                        ready_at=time.monotonic() + delay,
                        sequence=next(self._sequence),
                        path=task.path,
                        attempts=task.attempts + 1,
                    )
                )
        finally:
            self._queue.task_done()
        return True

    def stop(self) -> None:
        self._stop_event.set()

    def drain(self, *, timeout: float | None = None) -> bool:
        if not self.enabled:
            return True
        if timeout is None:
            self._queue.join()
            return True

        deadline = time.monotonic() + timeout
        with self._queue.all_tasks_done:
            while self._queue.unfinished_tasks:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._queue.all_tasks_done.wait(remaining)
        return True

    def pending_count(self) -> int:
        with self._pending_lock:
            return len(self._pending)

    def _enqueue_path(self, path: Path) -> bool:
        if not self.enabled or path.suffix.lower() != PNG_SUFFIX:
            return False
        resolved = path.resolve()
        with self._pending_lock:
            if resolved in self._pending:
                return False
            self._pending.add(resolved)
        self._queue.put(_QueuedUpload(ready_at=time.monotonic(), sequence=next(self._sequence), path=resolved))
        return True

    def _mark_not_pending(self, path: Path) -> None:
        with self._pending_lock:
            self._pending.discard(path.resolve())

    def _retry_delay(self, attempts: int) -> float:
        delay = self._retry_delay_seconds * (2 ** min(max(attempts - 1, 0), 10))
        return min(delay, self._max_retry_delay_seconds)


def _safe_stat(path: Path):
    try:
        return path.stat()
    except FileNotFoundError:
        return None
