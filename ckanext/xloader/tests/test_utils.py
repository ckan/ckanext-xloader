import pytest
from unittest.mock import patch

from ckanext.xloader import utils

@pytest.mark.parametrize("result_url, ckan_url, expected", [
    ("https://example.com/resource/123", "https://ckan.example.org", "https://ckan.example.org/resource/123"),
    ("https://ckan.example.org/resource/123", "https://ckan.example.org", "https://ckan.example.org/resource/123"),
    ("http://old-ckan.com/resource/456", "http://new-ckan.com", "http://new-ckan.com/resource/456"),
    ("https://sub.example.com/path", "https://ckan.example.com", "https://ckan.example.com/path"),
    ("ftp://fileserver.com/file", "https://ckan.example.com", "ftp://fileserver.com/file"),
    ("https://ckan.example.org/resource/789", "https://xloader.example.org", "https://xloader.example.org/resource/789"),
    ("https://ckan.example.org/dataset/data", "https://xloader.example.org", "https://xloader.example.org/dataset/data"),
    ("https://ckan.example.org/resource/123?foo=bar", "https://xloader.example.org", "https://xloader.example.org/resource/123?foo=bar"),
    ("https://ckan.example.org/dataset/456#section", "https://xloader.example.org", "https://xloader.example.org/dataset/456#section"),
    ("https://ckan.example.org/resource/123?param=value&other=123", "https://xloader.example.org", "https://xloader.example.org/resource/123?param=value&other=123"),
    ("https://ckan.example.org/resource/partial#fragment", "https://xloader.example.org", "https://xloader.example.org/resource/partial#fragment"),
    ("https://ckan.example.org/path/to/data?key=value#section", "https://xloader.example.org", "https://xloader.example.org/path/to/data?key=value#section"),
])
def test_modify_ckan_url(result_url, ckan_url, expected):
    assert utils.modify_ckan_url(result_url, ckan_url) == expected


def test_modify_ckan_url_no_change():
    url = "https://ckan.example.com/dataset"
    assert utils.modify_ckan_url(url, "https://ckan.example.com") == url


@pytest.mark.parametrize("orig_ckan_url, ckan_site_url, xloader_site_url, expected", [
    ("https://ckan.example.org/resource/789", "https://ckan.example.org", "https://xloader.example.org", "https://xloader.example.org/resource/789"),
    ("https://ckan.example.org/dataset/data", "https://ckan.example.org", "https://xloader.example.org", "https://xloader.example.org/dataset/data"),
    ("https://ckan.example.org/resource/123?foo=bar", "https://ckan.example.org", "https://xloader.example.org", "https://xloader.example.org/resource/123?foo=bar"),
    ("https://ckan.example.org/dataset/456#section", "https://ckan.example.org", "https://xloader.example.org", "https://xloader.example.org/dataset/456#section"),
    ("https://other-site.com/resource/999", "https://ckan.example.org", "https://xloader.example.org", "https://other-site.com/resource/999"),
    ("https://ckan.example.org/resource/123?param=value&other=123", "https://ckan.example.org", "https://xloader.example.org", "https://xloader.example.org/resource/123?param=value&other=123"),
    ("https://ckan.example.org/resource/partial#fragment", "https://ckan.example.org", "https://xloader.example.org", "https://xloader.example.org/resource/partial#fragment"),
    ("https://ckan.example.org/path/to/data?key=value#section", "https://ckan.example.org", "https://xloader.example.org", "https://xloader.example.org/path/to/data?key=value#section"),
])
def test_modify_resource_url(orig_ckan_url, ckan_site_url, xloader_site_url, expected):
    with patch.dict("your_module.toolkit.config", {"ckan.site_url": ckan_site_url, "ckanext.xloader.site_url": xloader_site_url}):
        assert utils.modify_resource_url(orig_ckan_url) == expected


def test_modify_resource_url_no_xloader_site():
    url = "https://ckan.example.org/dataset"
    with patch.dict("your_module.toolkit.config", {"ckan.site_url": "https://ckan.example.org", "ckanext.xloader.site_url": None}):
        assert utils.modify_resource_url(url) == url
