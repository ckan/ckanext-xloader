import pytest
import io
import os

from datetime import datetime

from requests import Response

from ckan.cli.cli import ckan
from ckan.plugins import toolkit
from ckan.tests import helpers, factories

from unittest import mock

from ckanext.xloader import jobs
from ckanext.xloader.utils import get_xloader_user_apitoken


_TEST_FILE_CONTENT = "x, y\n1,2\n2,4\n3,6\n4,8\n5,10"
_TEST_LARGE_FILE_CONTENT = "\n1,2\n2,4\n3,6\n4,8\n5,10"


def get_response(download_url, headers):
    """Mock jobs.get_response() method."""
    resp = Response()
    resp.raw = io.BytesIO(_TEST_FILE_CONTENT.encode())
    resp.headers = headers
    return resp


def get_large_response(download_url, headers):
    """Mock jobs.get_response() method to fake a large file."""
    resp = Response()
    resp.raw = io.BytesIO(_TEST_FILE_CONTENT.encode())
    resp.headers = {'content-length': 2000000000}
    return resp


def get_large_data_response(download_url, headers):
    """Mock jobs.get_response() method."""
    resp = Response()
    f_content = _TEST_FILE_CONTENT + (_TEST_LARGE_FILE_CONTENT * 500000)
    resp.raw = io.BytesIO(f_content.encode())
    resp.headers = headers
    return resp


def _get_temp_files(dir='/tmp'):
    return [os.path.join(dir, f) for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]


@pytest.fixture
def apikey():
    sysadmin = factories.SysadminWithToken()
    return sysadmin["token"]


@pytest.fixture
def data(create_with_upload, apikey):
    dataset = factories.Dataset()
    resource = create_with_upload(
        _TEST_FILE_CONTENT,
        "multiplication_2.csv",
        url="http://data",
        package_id=dataset["id"]
    )
    callback_url = toolkit.url_for(
        "api.action", ver=3, logic_function="xloader_hook", qualified=True
    )
    return {
        'api_key': apikey,
        'job_type': 'xloader_to_datastore',
        'result_url': callback_url,
        'metadata': {
            'ignore_hash': True,
            'ckan_url': toolkit.config.get('ckan.site_url'),
            'resource_id': resource["id"],
            'set_url_type': False,
            'task_created': datetime.utcnow().isoformat(),
            'original_url': resource["url"],
        }
    }


@pytest.mark.usefixtures("clean_db", "with_plugins")
@pytest.mark.ckan_config("ckanext.xloader.job_timeout", 2)
@pytest.mark.ckan_config("ckan.jobs.timeout", 2)
class TestXLoaderJobs(helpers.FunctionalRQTestBase):

    def test_xloader_data_into_datastore(self, cli, data):
        self.enqueue(jobs.xloader_data_into_datastore, [data])
        with mock.patch("ckanext.xloader.jobs.get_response", get_response):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            assert "File hash: d44fa65eda3675e11710682fdb5f1648" in stdout
            assert "Fields: [{'id': 'x', 'type': 'text', 'strip_extra_white': True}, {'id': 'y', 'type': 'text', 'strip_extra_white': True}]" in stdout
            assert "Copying to database..." in stdout
            assert "Creating search index..." in stdout
            assert "Express Load completed" in stdout

        resource = helpers.call_action("resource_show", id=data["metadata"]["resource_id"])
        assert resource["datastore_contains_all_records_of_source_file"]

    # Set the ckanext.xloader.site_url in the config
    @pytest.mark.ckan_config("ckanext.xloader.site_url", 'http://xloader-site-url')
    def test_download_resource_data_with_ckanext_xloader_site_url(self, cli, data):

        data['metadata']['original_url'] = 'http://xloader-site-url/resource.csv'
        self.enqueue(jobs.xloader_data_into_datastore, [data])
        with mock.patch("ckanext.xloader.jobs.get_response", get_response):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            assert "Express Load completed" in stdout

        resource = helpers.call_action("resource_show", id=data["metadata"]["resource_id"])
        assert resource["datastore_contains_all_records_of_source_file"]

    @pytest.mark.ckan_config("ckanext.site_url", 'http://ckan-site-url')
    def test_download_resource_data_with_ckan_site_url(self, cli, data):
        data['metadata']['original_url'] = 'http://ckan-site-url/resource.csv'
        self.enqueue(jobs.xloader_data_into_datastore, [data])
        with mock.patch("ckanext.xloader.jobs.get_response", get_response):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            assert "Express Load completed" in stdout

        resource = helpers.call_action("resource_show", id=data["metadata"]["resource_id"])
        assert resource["datastore_contains_all_records_of_source_file"]

    @pytest.mark.ckan_config("ckanext.site_url", 'http://ckan-site-url')
    def test_download_resource_data_with_different_original_url(self, cli, data):
        data['metadata']['original_url'] = 'http://external-site-url/resource.csv'
        self.enqueue(jobs.xloader_data_into_datastore, [data])
        with mock.patch("ckanext.xloader.jobs.get_response", get_response):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            assert "Express Load completed" in stdout

        resource = helpers.call_action("resource_show", id=data["metadata"]["resource_id"])
        assert resource["datastore_contains_all_records_of_source_file"]

    @pytest.mark.ckan_config("ckanext.xloader.site_url", 'http://xloader-site-url')
    def test_callback_xloader_hook_with_ckanext_xloader_site_url(self, cli, data):
        data['result_url'] = 'http://xloader-site-url/api/3/action/xloader_hook'
        self.enqueue(jobs.xloader_data_into_datastore, [data])
        with mock.patch("ckanext.xloader.jobs.get_response", get_response):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            assert "Express Load completed" in stdout

        resource = helpers.call_action("resource_show", id=data["metadata"]["resource_id"])
        assert resource["datastore_contains_all_records_of_source_file"]

    @pytest.mark.ckan_config("ckanext.site_url", 'http://ckan-site-url')
    def test_callback_xloader_hook_with_ckan_site_url(self, cli, data):
        data['result_url'] = 'http://ckan-site-url/api/3/action/xloader_hook'
        self.enqueue(jobs.xloader_data_into_datastore, [data])
        with mock.patch("ckanext.xloader.jobs.get_response", get_response):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            assert "Express Load completed" in stdout

        resource = helpers.call_action("resource_show", id=data["metadata"]["resource_id"])
        assert resource["datastore_contains_all_records_of_source_file"]

    def test_xloader_ignore_hash(self, cli, data):
        self.enqueue(jobs.xloader_data_into_datastore, [data])
        with mock.patch("ckanext.xloader.jobs.get_response", get_response):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            assert "Express Load completed" in stdout

        self.enqueue(jobs.xloader_data_into_datastore, [data])
        with mock.patch("ckanext.xloader.jobs.get_response", get_response):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            assert "Copying to database..." in stdout
            assert "Express Load completed" in stdout

        data["metadata"]["ignore_hash"] = False
        self.enqueue(jobs.xloader_data_into_datastore, [data])
        with mock.patch("ckanext.xloader.jobs.get_response", get_response):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            assert "Ignoring resource - the file hash hasn't changed" in stdout

    def test_data_too_big_error_if_content_length_bigger_than_config(self, cli, data):
        self.enqueue(jobs.xloader_data_into_datastore, [data])
        with mock.patch("ckanext.xloader.jobs.get_response", get_large_response):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            assert "Data too large to load into Datastore:" in stdout

    def test_data_max_excerpt_lines_config(self, cli, data):
        self.enqueue(jobs.xloader_data_into_datastore, [data])
        with mock.patch("ckanext.xloader.jobs.get_response", get_large_response):
            with mock.patch("ckanext.xloader.jobs.MAX_EXCERPT_LINES", 1):
                stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
                assert "Loading excerpt of ~1 lines to DataStore." in stdout

        resource = helpers.call_action("resource_show", id=data["metadata"]["resource_id"])
        assert resource["datastore_contains_all_records_of_source_file"] is False

    def test_data_with_rq_job_timeout(self, cli, data):
        file_suffix = 'multiplication_2.csv'
        self.enqueue(jobs.xloader_data_into_datastore, [data], rq_kwargs=dict(timeout=2))
        with mock.patch("ckanext.xloader.jobs.get_response", get_large_data_response):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            assert "Job timed out after" in stdout
            for f in _get_temp_files():
                # make sure that the tmp file has been closed/deleted in job timeout exception handling
                assert file_suffix not in f

    @pytest.mark.parametrize("error_type,should_retry", [
        # Retryable errors from RETRYABLE_ERRORS
        ("DeadlockDetected", True),
        ("LockNotAvailable", True), 
        ("ObjectInUse", True),
        ("XLoaderTimeoutError", True),
        # Retryable HTTP errors (status codes from is_retryable_error)
        ("HTTPError_408", True),
        ("HTTPError_429", True),
        ("HTTPError_500", True),
        ("HTTPError_502", True),
        ("HTTPError_503", True),
        ("HTTPError_504", True),
        ("HTTPError_507", True),
        ("HTTPError_522", True),
        ("HTTPError_524", True),
        # Non-retryable HTTP errors
        ("HTTPError_400", False),
        ("HTTPError_404", False),
        ("HTTPError_403", False),
        # Other non-retryable errors (not in RETRYABLE_ERRORS)
        ("ValueError", False),
        ("TypeError", False),
    ])
    def test_retry_behavior(self, cli, data, error_type, should_retry):
        """Test retry behavior for different error types."""
        
        def create_mock_error(error_type):
            if error_type == "DeadlockDetected":
                from psycopg2 import errors
                return errors.DeadlockDetected()
            elif error_type == "LockNotAvailable":
                from psycopg2 import errors
                return errors.LockNotAvailable()
            elif error_type == "ObjectInUse":
                from psycopg2 import errors
                return errors.ObjectInUse()
            elif error_type == "XLoaderTimeoutError":
                return jobs.XLoaderTimeoutError('Connection timed out after 30s')
            elif error_type.startswith("HTTPError_"):
                status_code = int(error_type.split("_")[1])
                return jobs.HTTPError("HTTP Error", status_code=status_code, request_url="test", response=None)
            elif error_type == "ValueError":
                return ValueError("Test error")
            elif error_type == "TypeError":
                return TypeError("Test error")
        
        def mock_download_with_error(*args, **kwargs):
            if not hasattr(mock_download_with_error, 'call_count'):
                mock_download_with_error.call_count = 0
            mock_download_with_error.call_count += 1
            
            if mock_download_with_error.call_count == 1:
                # First call - raise the test error
                raise create_mock_error(error_type)
            elif should_retry:
                # Second call - return successful response only if retryable
                import tempfile
                tmp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv')
                tmp_file.write(_TEST_FILE_CONTENT)
                tmp_file.flush()
                return (tmp_file, 'd44fa65eda3675e11710682fdb5f1648')
            else:
                # Non-retryable errors should not get a second chance
                raise create_mock_error(error_type)
        
        self.enqueue(jobs.xloader_data_into_datastore, [data])
        
        with mock.patch("ckanext.xloader.jobs._download_resource_data", mock_download_with_error):
            stdout = cli.invoke(ckan, ["jobs", "worker", "--burst"]).output
            
            if should_retry:
                # Check that retry was attempted
                assert "Job failed due to temporary error" in stdout
                assert "retrying" in stdout
                assert "Express Load completed" in stdout
                # Verify resource was successfully loaded after retry
                resource = helpers.call_action("resource_show", id=data["metadata"]["resource_id"])
                assert resource["datastore_contains_all_records_of_source_file"]
            else:
                # Check that job failed without retry - should have error messages
                assert "xloader error:" in stdout or "error" in stdout.lower()
                assert "Express Load completed" not in stdout


@pytest.mark.usefixtures("clean_db")
class TestSetResourceMetadata(object):
    def test_simple(self):
        resource = factories.Resource()

        jobs.set_resource_metadata(
            {
                "datastore_contains_all_records_of_source_file": True,
                "datastore_active": True,
                "ckan_url": "http://www.ckan.org/",
                "resource_id": resource["id"],
            }
        )

        resource = helpers.call_action("resource_show", id=resource["id"])
        assert resource["datastore_contains_all_records_of_source_file"]
        assert resource["datastore_active"]
        assert resource["ckan_url"] == "http://www.ckan.org/"
