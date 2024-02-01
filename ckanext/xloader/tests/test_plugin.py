# encoding: utf-8

import datetime
import pytest
try:
    from unittest import mock
except ImportError:
    import mock
from six import text_type as str
from ckan.tests import helpers, factories
from ckan.logic import _actions
from ckan.plugins import toolkit
from ckanext.xloader.plugin import _should_remove_unsupported_resource_from_datastore


@pytest.fixture
def toolkit_config_value(request):
    _original_config = toolkit.config.copy()
    toolkit.config['ckanext.xloader.clean_datastore_tables'] = request.param
    try:
        yield
    finally:
        toolkit.config.clear()
        toolkit.config.update(_original_config)


@pytest.fixture
def mock_xloader_formats(request):
    with mock.patch('ckanext.xloader.plugin.XLoaderFormats.is_it_an_xloader_format') as mock_is_xloader_format:
        mock_is_xloader_format.return_value = request.param
        yield mock_is_xloader_format


@pytest.mark.usefixtures("clean_db", "with_plugins")
@pytest.mark.ckan_config("ckan.plugins", "datastore xloader")
class TestNotify(object):
    def test_submit_on_resource_create(self, monkeypatch):
        func = mock.Mock()
        monkeypatch.setitem(_actions, "xloader_submit", func)

        dataset = factories.Dataset()

        assert not func.called

        helpers.call_action(
            "resource_create",
            {},
            package_id=dataset["id"],
            url="http://example.com/file.csv",
            format="CSV",
        )

        assert func.called

    def test_submit_when_url_changes(self, monkeypatch):
        func = mock.Mock()
        monkeypatch.setitem(_actions, "xloader_submit", func)

        dataset = factories.Dataset()

        resource = helpers.call_action(
            "resource_create",
            {},
            package_id=dataset["id"],
            url="http://example.com/file.pdf",
        )

        assert not func.called  # because of the format being PDF

        helpers.call_action(
            "resource_update",
            {},
            id=resource["id"],
            package_id=dataset["id"],
            url="http://example.com/file.csv",
            format="CSV",
        )

        assert func.called

    @pytest.mark.usefixtures("toolkit_config_value", "mock_xloader_formats")
    @pytest.mark.parametrize("toolkit_config_value, mock_xloader_formats, url_type, datastore_active, expected_result",
                             [(True, False, 'upload', True, True),  # Test1
                              (True, True, 'upload', True, False),  # Test2
                              (False, False, 'upload', True, False),  # Test3
                              (True, False, 'custom_type', True, False),  # Test4
                              (True, False, 'upload', False, False),  # Test5
                              (True, False, '', True, True),  # Test6
                              (True, False, None, True, True),  # Test7
                             ], indirect=["toolkit_config_value", "mock_xloader_formats"])
    def test_should_remove_unsupported_resource_from_datastore(
        toolkit_config_value, mock_xloader_formats, url_type, datastore_active, expected_result):

        # Test1: clean_datastore_tables=True, is_it_an_xloader_format=True, url_type='upload', datastore_active=True, expected_result=True
        #   Should pass as it is not an XLoader format and supported url type and datastore active.
        # Test2: clean_datastore_tables=True, is_it_an_xloader_format=False, url_type='upload', datastore_active=True, expected_result=False
        #   Should fail as it is a supported XLoader format.
        # Test3: clean_datastore_tables=False, is_it_an_xloader_format=True, url_type='upload', datastore_active=True, expected_result=False
        #   Should fail as the config option is turned off.
        # Test4: clean_datastore_tables=True, is_it_an_xloader_format=True, url_type='custom_type', datastore_active=True, expected_result=False
        #   Should fail as the url_type is not supported.
        # Test5: clean_datastore_tables=True, is_it_an_xloader_format=True, url_type='upload', datastore_active=False, expected_result=False
        #   Should fail as datastore is inactive.
        # Test6: clean_datastore_tables=True, is_it_an_xloader_format=True, url_type='', datastore_active=True, expected_result=True
        #   Should pass as it is not an XLoader format and supported url type and datastore active.
        # Test7: clean_datastore_tables=True, is_it_an_xloader_format=True, url_type=None, datastore_active=True, expected_result=True
        #   Should pass as it is not an XLoader format and supported url type as falsy and datastore active.

        # Setup mock data
        res_dict = {
            'format': 'some_format',
            'url_type': url_type,
            'datastore_active': datastore_active,
            'extras': {'datastore_active': datastore_active}
        }

        # Assert the result based on the logic paths covered
        assert _should_remove_unsupported_resource_from_datastore(res_dict) == expected_result

    def _pending_task(self, resource_id):
        return {
            "entity_id": resource_id,
            "entity_type": "resource",
            "task_type": "xloader",
            "last_updated": str(datetime.datetime.utcnow()),
            "state": "pending",
            "key": "xloader",
            "value": "{}",
            "error": "{}",
        }
