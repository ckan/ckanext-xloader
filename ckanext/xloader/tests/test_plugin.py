import datetime
import pytest
import mock
import ckan.plugins as p
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
