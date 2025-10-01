"""Network helpers for the scheduler."""

from __future__ import annotations

import socket

import urllib3.util.connection as urllib3_connection


def force_ipv4_connections() -> None:
    """Force requests/urllib3 to use IPv4 only.

    Some deployments sit behind resolvers that return IPv6 first but the route
    is unreliable. When that happens, downloads intermittently fail with
    ``Can't assign requested address``. By overriding urllib3's address family
    resolution we ensure all outgoing radar fetches use IPv4, matching the
    behaviour of the legacy implementation.
    """

    def allowed_gai_family():
        return socket.AF_INET

    urllib3_connection.allowed_gai_family = allowed_gai_family
    setattr(urllib3_connection, "HAS_IPV6", False)

    try:
        import requests.packages.urllib3.util.connection as requests_urllib3_connection
    except ImportError:
        requests_urllib3_connection = None

    if requests_urllib3_connection is not None:
        requests_urllib3_connection.allowed_gai_family = allowed_gai_family
        setattr(requests_urllib3_connection, "HAS_IPV6", False)



