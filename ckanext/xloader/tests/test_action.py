import json
import pytest

try:
    from unittest import mock
except ImportError:
    import mock

from ckan.plugins.toolkit import NotAuthorized, url_for
from ckan.tests import helpers, factories

from ckanext.xloader.utils import get_xloader_user_apitoken

from ckanext.xloader.jobs import xloader_data_into_datastore


@pytest.mark.usefixtures("clean_db", "with_plugins")
@pytest.mark.ckan_config("ckan.plugins", "datastore xloader")
class TestAction(object):
    def test_submit(self):
        # checks that xloader_submit enqueues the resource (to be xloadered)
        user = factories.User()
        # normally creating a resource causes xloader_submit to be called,
        # but we avoid that by setting an invalid format
        res = factories.Resource(user=user, format="aaa")
        # mock the enqueue
        with mock.patch(
            "ckanext.xloader.action.enqueue_job",
            return_value=mock.MagicMock(id=123),
        ) as enqueue_mock:
            helpers.call_action(
                "xloader_submit",
                context=dict(user=user["name"]),
                resource_id=res["id"],
            )
            assert 1 == enqueue_mock.call_count

    def test_submit_to_custom_queue_without_auth(self):
        # check that xloader_submit doesn't allow regular users to change queues
        user = factories.User()
        with pytest.raises(NotAuthorized):
            helpers.call_auth(
                "xloader_submit",
                context=dict(user=user["name"], model=None),
                queue='foo',
            )

    def test_submit_to_custom_queue_as_sysadmin(self):
        # check that xloader_submit allows sysadmins to change queues
        user = factories.Sysadmin()
        assert helpers.call_auth(
            "xloader_submit",
            context=dict(user=user["name"], model=None),
            queue='foo',
        ) is True

    def test_duplicated_submits(self):
        def submit(res, user):
            return helpers.call_action(
                "xloader_submit",
                context=dict(user=user["name"]),
                resource_id=res["id"],
            )

        user = factories.User()

        with mock.patch(
            "ckanext.xloader.action.enqueue_job",
            return_value=mock.MagicMock(id=123),
        ) as enqueue_mock:
            enqueue_mock.reset_mock()
            # creating the resource causes it to be queued
            res = factories.Resource(user=user, format="csv")
            assert 1 == enqueue_mock.call_count
            # a second request to queue it will be stopped, because of the
            # existing task for this resource - shown by task_status_show
            submit(res, user)
            assert 1 == enqueue_mock.call_count

    def test_xloader_hook(self):
        # Check the task_status is stored correctly after a xloader job.
        user = factories.User()
        res = factories.Resource(user=user, format="csv")
        task_status = helpers.call_action(
            "task_status_update",
            context={},
            entity_id=res["id"],
            entity_type="resource",
            task_type="xloader",
            key="xloader",
            value="{}",
            error="{}",
            state="pending",
        )

        helpers.call_action(
            "xloader_hook",
            context=dict(user=user["name"]),
            metadata={"resource_id": res["id"]},
            status="complete",
        )

        task_status = helpers.call_action(
            "task_status_show",
            context={},
            entity_id=res["id"],
            task_type="xloader",
            key="xloader",
        )
        assert task_status["state"] == "complete"

    def test_status(self):

        # Trigger an xloader job
        res = factories.Resource(format="CSV")

        status = helpers.call_action(
            "xloader_status",
            resource_id=res["id"],
        )

        assert status["status"] == "pending"

    def test_status_actual_job(self):

        res = factories.Resource(format="CSV")
        with mock.patch(
            "ckanext.xloader.jobs.xloader_data_into_datastore_",

        ):
            helpers.call_action(
                "xloader_submit",
                resource_id=res["id"],
                sync=True
            )
            existing_task = helpers.call_action("task_status_show", entity_id=res["id"], task_type="xloader",
                key="xloader"
            )

            job_id = json.loads(existing_task["value"])["job_id"]
            with mock.patch(
                "ckanext.xloader.jobs.get_current_job",
                return_value=mock.MagicMock(id=job_id),
            ):
                # Manually trigger an xloader job to actually populate the jobs table
                # in the xloader db
                callback_url = url_for(
                    "api.action", ver=3, logic_function="xloader_hook", qualified=True
                )
                xloader_data_into_datastore(
                    {
                        "metadata": {"resource_id": res["id"], "ckan_url": url_for("/")},
                        "result_url": callback_url,
                        "api_key": "xxx",
                        "job_type": "xloader_to_datastore",
                    }
                )

            status = helpers.call_action(
                "xloader_status",
                resource_id=res["id"],
            )

            assert status["job_id"] == job_id


    def test_xloader_user_api_token_defaults_to_site_user_apikey(self):
        api_token = get_xloader_user_apitoken()
        site_user = helpers.call_action("get_site_user")
        assert api_token == site_user["apikey"]

    @pytest.mark.ckan_config("ckanext.xloader.api_token", "random-api-token")
    def test_xloader_user_api_token(self):
        api_token = get_xloader_user_apitoken()

        assert api_token == "random-api-token"
