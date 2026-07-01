from __future__ import annotations

import logging
from typing import Any

import pytest
import sqlalchemy as sa
from faker import Faker

from ckanext.xloader import db, jobs


@pytest.mark.usefixtures("with_plugins", "clean_db")
class TestGetJob:
    @pytest.fixture(autouse=True)
    def init(self, ckan_config: dict[str, Any]):
        db.init(ckan_config)
        # tables may persist across test sessions. Remove old records to
        # prevent UNIQUE job_id errors.
        with db.ENGINE.begin() as conn:
            conn.execute(sa.delete(db.JOBS_TABLE))
            conn.execute(sa.delete(db.METADATA_TABLE))
            conn.execute(sa.delete(db.LOGS_TABLE))

    def test_jobs_table_not_initialized(
        self, faker: Faker, monkeypatch: pytest.MonkeyPatch
    ):
        """When jobs table is not initialized, get_job raises a RuntimeError."""
        monkeypatch.setattr(db, "JOBS_TABLE", None)
        with pytest.raises(RuntimeError):
            db.get_job(faker.uuid4())

    def test_metadata_table_not_initialized(
        self, faker: Faker, monkeypatch: pytest.MonkeyPatch
    ):
        """When metadata table is not initialized, get_job raises a RuntimeError.

        This should not happen normally, but since we are using global
        variables, there is always a chance that somebody hacks things in a
        wrong way and we must react properly.
        """
        id = faker.uuid4()
        db.add_pending_job(id, "test", "123")

        monkeypatch.setattr(db, "METADATA_TABLE", None)
        with pytest.raises(RuntimeError):
            db.get_job(id)

    def test_logs_table_not_initialized(
        self, faker: Faker, monkeypatch: pytest.MonkeyPatch
    ):
        """When logs table is not initialized, get_job raises a RuntimeError.

        This should not happen normally, but since we are using global
        variables, there is always a chance that somebody hacks things in a
        wrong way and we must react properly.
        """
        id = faker.uuid4()
        db.add_pending_job(id, "test", "123")

        monkeypatch.setattr(db, "LOGS_TABLE", None)
        with pytest.raises(RuntimeError):
            db.get_job(id)

    def test_initialized(self, faker: Faker):
        """get_job returns None for a non-existent job."""
        job = db.get_job(faker.uuid4())
        assert job is None

    def test_existing(self, faker: Faker):
        """get_job returns a job dict for an existing job."""
        id = faker.uuid4()
        metadata = faker.pydict(value_types=(int, str))

        db.add_pending_job(id, "test", "123", metadata=metadata)

        job = db.get_job(id)
        assert job
        assert job["job_type"] == "test"
        assert job["metadata"] == metadata

    def test_logs(self, faker: Faker):
        """get_job returns logs for an existing job, if there are any."""
        id = faker.uuid4()
        db.add_pending_job(id, "test", "123")
        job = db.get_job(id)
        assert not job["logs"]

        handler = jobs.StoringHandler(id, {})
        handler.setLevel(logging.DEBUG)
        logger = logging.getLogger(id)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)

        first_message = faker.sentence()
        second_message = faker.sentence()

        logger.info(first_message)
        logger.info(second_message)

        job = db.get_job(id)
        assert len(job["logs"]) == 2

        messages = sorted([item["message"] for item in job["logs"]])

        assert messages == sorted([first_message, second_message])
