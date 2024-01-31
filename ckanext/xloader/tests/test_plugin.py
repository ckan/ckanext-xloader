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
from ckanext.xloader.plugin import _should_remove_unsupported_resource_from_datastore


@pytest.fixture
def mock_toolkit_config(request):
    with mock.patch('ckan.plugins.toolkit.config.get') as mock_get:
        mock_get.return_value = request.params
        yield mock_get


@pytest.fixture
def mock_xloader_formats(request):
    with mock.patch('ckanext.xloader.plugin.XLoaderFormats.is_it_an_xloader_format') as mock_is_xloader_format:
        mock_is_xloader_format.return_value = request.params
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

    @pytest.mark.parametrize("toolkit_config_value, xloader_formats_value, url_type, datastore_active, expected_result",
                             [(True, True, 'upload', True, True),  # Test1
                              (True, False, 'upload', True, False),  # Test2
                              (False, True, 'upload', True, False),  # Test3
                              (False, False, 'upload', True, False),  # Test4
                              (True, True, 'custom_type', True, False),  # Test5
                              (True, True, 'upload', False, False),  # Test6
                              (True, True, '', True, True),  # Test7
                              (True, True, None, True, True),  # Test8
                             ])
    def test_should_remove_unsupported_resource_from_datastore(
        mock_toolkit_config, mock_xloader_formats, toolkit_config_value,
        xloader_formats_value, url_type, datastore_active, expected_result):

        # Test1: clean_datastore_tables=True, is_it_an_xloader_format=True, url_type='upload', datastore_active=True, expected_result=True
        #   Should pass as it is an Xloader format and supported url type and datastore active.
        # Test2: clean_datastore_tables=True, is_it_an_xloader_format=False, url_type='upload', datastore_active=True, expected_result=False
        #   Should fail as it is not a supported Xloader format.
        # Test3: clean_datastore_tables=False, is_it_an_xloader_format=True, url_type='upload', datastore_active=True, expected_result=False
        #   Should fail as the config option is turned off.
        # Test4: clean_datastore_tables=False, is_it_an_xloader_format=False, url_type='upload', datastore_active=True, expected_result=False
        #   Should fail as the config option is turned off and the Xloader format is not supported.
        # Test5: clean_datastore_tables=True, is_it_an_xloader_format=True, url_type='custom_type', datastore_active=True, expected_result=False
        #   Should fail as the url_type is not supported.
        # Test6: clean_datastore_tables=True, is_it_an_xloader_format=True, url_type='upload', datastore_active=False, expected_result=False
        #   Should fail as datastore is inactive.
        # Test7: clean_datastore_tables=True, is_it_an_xloader_format=True, url_type='', datastore_active=True, expected_result=True
        #   Should pass as it is an Xloader format and supported url type and datastore active.
        # Test8: clean_datastore_tables=True, is_it_an_xloader_format=True, url_type=None, datastore_active=True, expected_result=True
        #   Should pass as it is an Xloader format and supported url type as falsy and datastore active.

        # Setup mock data
        res_dict = {
            'format': 'some_format',
            'url_type': url_type,
            'datastore_active': True,
            'extras': {'datastore_active': True}
        }

        # Call the function
        result = _should_remove_unsupported_resource_from_datastore(res_dict)

        # Assert the result based on the logic paths covered
        assert result == expected_result

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
