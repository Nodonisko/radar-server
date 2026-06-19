"""Per-source input fetchers.

Each module implements discovery (and, where needed, download/materialization)
for one source type configured in :mod:`radar_server.config`:

- :mod:`.http_directory` -- plain HTTP directory listings (``HttpDirectorySource``)
- :mod:`.ord` -- the EUMETNET ORD/OPERA API (``OrdApiSource``)
- :mod:`.arco` -- MeteoHub ARCO Zarr stores (``ArcoZarrSource``)

Shared records, errors, and HTTP helpers live in :mod:`.base`. The
:mod:`radar_server.fetching` orchestrator dispatches to these modules by source
type. New providers should add a module here and a branch in the orchestrator.
"""

from . import arco, http_directory, ord
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
    "FetchError",
    "RateLimitedError",
    "SkippableFetchError",
    "RemoteInputFile",
    "LocalInputFile",
]
