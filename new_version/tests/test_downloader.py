from types import SimpleNamespace

from new_version import downloader


def test_list_remote_files_includes_tar(monkeypatch):
    html_listing = """
    <html>
        <body>
            <a href="/data/z_forecast.tar">forecast</a>
            <a href="y_radar.hdf">latest radar</a>
            <a href="a_old.hdf">old radar</a>
            <a href="irrelevant.txt">ignore</a>
        </body>
    </html>
    """

    dummy_response = SimpleNamespace(text=html_listing)

    def fake_request(url: str, stream: bool = False):  # noqa: ARG001
        return dummy_response

    monkeypatch.setattr(downloader, "_request_with_retry", fake_request)

    entries = downloader.list_remote_files("https://example.test/")

    assert entries == ["z_forecast.tar", "y_radar.hdf", "a_old.hdf"]


