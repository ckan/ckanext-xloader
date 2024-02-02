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

    @pytest.mark.ckan_config("ckanext.xloader.validation.requires_successful_report", True)
    def test_require_validation(self, monkeypatch):
        func = mock.Mock()
        monkeypatch.setitem(_actions, "xloader_submit", func)

        mock_resource_validation_show = mock.Mock()
        monkeypatch.setitem(_actions, "resource_validation_show", mock_resource_validation_show)

        dataset = factories.Dataset()

        resource = helpers.call_action(
            "resource_create",
            {},
            package_id=dataset["id"],
            url="http://example.com/file.csv",
            format="CSV",
            validation_status='failure',
        )

        assert not func.called  # because of the validation_status not being `success`
        func.called = None # reset

        helpers.call_action(
            "resource_update",
            {},
            id=resource["id"],
            package_id=dataset["id"],
            url="http://example.com/file2.csv",
            format="CSV",
            validation_status='success',
        )

        assert func.called  # because of the validation_status is `success`

    @pytest.mark.ckan_config("ckanext.xloader.validation.requires_successful_report", True)
    @pytest.mark.ckan_config("ckanext.xloader.validation.enforce_schema", False)
    def test_enforce_validation_schema(self, monkeypatch):
        func = mock.Mock()
        monkeypatch.setitem(_actions, "xloader_submit", func)

        mock_resource_validation_show = mock.Mock()
        monkeypatch.setitem(_actions, "resource_validation_show", mock_resource_validation_show)

        dataset = factories.Dataset()

        resource = helpers.call_action(
            "resource_create",
            {},
            package_id=dataset["id"],
            url="http://example.com/file.csv",
            schema='',
            validation_status='',
        )

        assert func.called  # because of the schema being empty
        func.called = None # reset

        helpers.call_action(
            "resource_update",
            {},
            id=resource["id"],
            package_id=dataset["id"],
            url="http://example.com/file2.csv",
            schema='https://example.com/schema.json',
            validation_status='failure',
        )

        assert not func.called  # because of the validation_status not being `success` and there is a schema
        func.called = None # reset

        helpers.call_action(
            "resource_update",
            {},
            package_id=dataset["id"],
            id=resource["id"],
            url="http://example.com/file3.csv",
            schema='https://example.com/schema.json',
            validation_status='success',
        )

        assert func.called  # because of the validation_status is `success` and there is a schema

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
