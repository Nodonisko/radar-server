"""Per-source input fetchers.

Each module implements discovery (and, where needed, download/materialization)
for one source type configured in :mod:`radar_server.config`:

- :mod:`.http_directory` -- plain HTTP directory listings (``HttpDirectorySource``)
- :mod:`.ord` -- the EUMETNET ORD/OPERA API (``OrdApiSource``)
- :mod:`.s3_bucket` -- public S3 ListObjectsV2 buckets (``S3BucketSource``)
- :mod:`.arco` -- MeteoHub ARCO Zarr stores (``ArcoZarrSource``)

Shared records, errors, and HTTP helpers live in :mod:`.base`. The
:mod:`radar_server.fetching` orchestrator dispatches to these modules by source
type. New providers should add a module here and a branch in the orchestrator.
"""

from . import arco, http_directory, ord, s3_bucket
from .base import (
    FetchError,
    LocalInputFile,
    RateLimitedError,
    RemoteInputFile,
    SkippableFetchError,
)

__all__ = [
    "arco",
    "http_directory",
    "ord",
    "s3_bucket",
    "FetchError",
    "RateLimitedError",
    "SkippableFetchError",
    "RemoteInputFile",
    "LocalInputFile",
]
