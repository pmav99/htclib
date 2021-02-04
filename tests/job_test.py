import pathlib

from typing import Any
from typing import Dict

import pytest

import htclib

# DEFAULT_ARGUMENTS: Mapping[str, Union[str, pathlib.Path, bool]] = dict(
DEFAULT_ARGUMENTS: Dict[str, Any] = dict(
    path=pathlib.Path("/tmp/htclib.job"),
    docker_image="htclib:latest",
    executable="/usr/bin/cat",
    arguments="/etc/fstab",
    proc_user="htclibproc",
)


@pytest.fixture
def tmp_job(tmp_path: pathlib.Path) -> htclib.HTCondorJob:
    data = DEFAULT_ARGUMENTS.copy()
    data["path"] = tmp_path / "htclib.job"
    job = htclib.HTCondorJob(**data)
    return job


@pytest.mark.parametrize(
    "path",
    [
        pytest.param(pathlib.Path("/tmp/foo.job"), id="path with suffix"),
        pytest.param(pathlib.Path("/tmp/foo"), id="path without suffix"),
    ],
)
def test_log_prefix_dynamic_default(path: pathlib.Path) -> None:
    """ Check that the log_prefix is set dynamically when it is omitted """
    data = DEFAULT_ARGUMENTS.copy()
    data["path"] = path
    data.update(path=path)
    job = htclib.HTCondorJob(**data)
    assert job.log_prefix == path.parent / path.stem


def test_log_prefix_explicit_value() -> None:
    data = DEFAULT_ARGUMENTS.copy()
    data["log_prefix"] = pathlib.Path("log_prefix")
    job = htclib.HTCondorJob(**data)
    assert job.log_prefix == data["log_prefix"]


def test_str_representation(tmp_job: htclib.HTCondorJob) -> None:
    str_job = str(tmp_job)
    assert str_job.startswith("universe")
    assert str_job.endswith("queue 1")
    assert len(str_job.splitlines()) > 10


def test_save_job(tmp_job: htclib.HTCondorJob) -> None:
    # check that the job description file gets created
    assert not tmp_job.path.exists()
    htclib.save_job(tmp_job)
    assert tmp_job.path.exists()
    # Check that the contents of the description file are the string representation of the job object
    assert str(tmp_job) == tmp_job.path.read_text()
