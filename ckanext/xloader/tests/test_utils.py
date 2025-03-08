import pytest
from unittest.mock import patch
from ckan.plugins import toolkit
from ckanext.xloader import utils

def test_private_modify_url_no_change():
    url = "https://ckan.example.com/dataset"
    assert utils._modify_url(url, "https://ckan.example.com") == url


@pytest.mark.parametrize("result_url, ckan_url, expected", [
    ("https://example.com/resource/123", "https://ckan.example.org", "https://ckan.example.org/resource/123"),
    ("https://example.com/resource/123", "http://127.0.0.1:3001", "http://127.0.0.1:3001/resource/123"),
    ("https://example.com/resource/123", "http://127.0.0.1:3001/pathnotadded", "http://127.0.0.1:3001/resource/123"),
    ("https://ckan.example.org/resource/123", "https://ckan.example.org", "https://ckan.example.org/resource/123"),
    ("http://old-ckan.com/resource/456", "http://new-ckan.com", "http://new-ckan.com/resource/456"),
    ("https://sub.example.com/path", "https://ckan.example.com", "https://ckan.example.com/path"),
    ("ftp://fileserver.com/file", "https://ckan.example.com", "ftp://fileserver.com/file"), ##should never happen
    ("https://ckan.example.org/resource/789", "https://xloader.example.org", "https://xloader.example.org/resource/789"),
    ("https://ckan.example.org/dataset/data", "https://xloader.example.org", "https://xloader.example.org/dataset/data"),
    ("https://ckan.example.org/resource/123?foo=bar", "https://xloader.example.org", "https://xloader.example.org/resource/123?foo=bar"),
    ("https://ckan.example.org/dataset/456#section", "https://xloader.example.org", "https://xloader.example.org/dataset/456#section"),
    ("https://ckan.example.org/resource/123?param=value&other=123", "https://xloader.example.org", "https://xloader.example.org/resource/123?param=value&other=123"),
    ("https://ckan.example.org/resource/partial#fragment", "https://xloader.example.org", "https://xloader.example.org/resource/partial#fragment"),
    ("https://ckan.example.org/path/to/data?key=value#section", "https://xloader.example.org", "https://xloader.example.org/path/to/data?key=value#section"),
    ("", "", ""),
    ("", "http://127.0.0.1:5000", ""),
    (None, None, None),
    (None, "http://127.0.0.1:5000", None),
])
def test_private_modify_url(result_url, ckan_url, expected):
    assert utils._modify_url(result_url, ckan_url) == expected


@pytest.mark.parametrize("input_url, ckan_site_url, xloader_site_url, is_altered, expected", [
    ("https://ckan.example.org/resource/789", "https://ckan.example.org", "https://xloader.example.org", True, "https://xloader.example.org/resource/789"),
    ("https://ckan.example.org/resource/789", "https://ckan.example.org", "http://127.0.0.1:3012", True, "http://127.0.0.1:3012/resource/789"),
    ("https://ckan.example.org/dataset/data", "https://ckan.example.org", "https://xloader.example.org", True, "https://xloader.example.org/dataset/data"),
    ("https://ckan.example.org/resource/123?foo=bar", "https://ckan.example.org", "https://xloader.example.org", True, "https://xloader.example.org/resource/123?foo=bar"),
    ("https://ckan.example.org/dataset/456#section", "https://ckan.example.org", "https://xloader.example.org", True, "https://xloader.example.org/dataset/456#section"),
    ("https://other-site.com/resource/999", "https://ckan.example.org", "https://xloader.example.org", False, ""),
    ("https://ckan.example.org/resource/123?param=value&other=123", "https://ckan.example.org", "https://xloader.example.org", True, "https://xloader.example.org/resource/123?param=value&other=123"),
    ("https://ckan.example.org/resource/partial#fragment", "https://ckan.example.org", "https://xloader.example.org", True, "https://xloader.example.org/resource/partial#fragment"),
    ("https://ckan.example.org/path/to/data?key=value#section", "https://ckan.example.org", "https://xloader.example.org", True, "https://xloader.example.org/path/to/data?key=value#section"),
    ("https://ckan.example.org/path/to/data?key=value#section", "https://ckan.example.org", "http://localhost:3000", True, "http://localhost:3000/path/to/data?key=value#section"),
    ("https://ckan.example.org/blackListedPathToS3HostOrigin?key=value#section", "https://ckan.example.org", "https://xloader.example.org", False, ""),
    ("ftp://ckan.example.org/dataset/456#section", "https://ckan.example.org", "https://xloader.example.org", False, ""),
    ("https://ckan.example.org/dataset/456#section", "https://ckan.example.org", "", False, ""),
    ("", "http://127.0.0.1:5000", None, False, ""),
    ("", "http://127.0.0.1:5000", "", False, ""),
    (None, "http://127.0.0.1:5000", None, False, ""),
    (None, "http://127.0.0.1:5000", "", False, ""),
])
def test_modify_input_url(input_url, ckan_site_url, xloader_site_url, is_altered, expected):
    with patch.dict(toolkit.config,
                    {"ckan.site_url": ckan_site_url,
                     "ckanext.xloader.site_url": xloader_site_url,
                     "ckanext.xloader.site_url_ignore_path_regex": "(/blackListedPathToS3HostOrigin|/anotherpath)"}):
        response = utils.modify_input_url(input_url)
        if is_altered:
            assert response == expected
        else:
            assert response == input_url



def test_modify_input_url_no_xloader_site():
    url = "https://ckan.example.org/dataset"
    with patch.dict(toolkit.config, {"ckan.site_url": "https://ckan.example.org", "ckanext.xloader.site_url": None}):
        assert utils.modify_input_url(url) == url
