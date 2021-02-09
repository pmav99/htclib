import pathlib
import shlex
import subprocess

from typing import Any
from typing import Dict
from typing import Optional

from jinja2 import Template
from pydantic import BaseModel
from pydantic import Field
from pydantic import validator


TEMPLATE = Template(
    """\
universe                = docker
docker_image            = {{ docker_image }}
executable              = {{ executable }}
arguments               = {{ arguments }}
{% if stdout -%}
output                  = {{ log_prefix }}.$(ClusterId).$(Process).out
{% endif -%}
{% if stderr -%}
error                   = {{ log_prefix }}.$(ClusterId).$(Process).err
{% endif -%}
log                     = {{ log_prefix }}.$(ClusterId).$(Process).log
request_memory          = {{ memory }}
{% if input_files -%}
transfer_input_files    = {{ input_files }}
{% endif -%}
should_transfer_files   = YES
when_to_transfer_output = ON_EXIT
queue {{ queue }}
""".strip()
)


class HTCondorJob(BaseModel):
    # fields related to the action of submitting the job
    path: pathlib.Path = Field(...)
    proc_user: str = Field(...)
    stdout: bool = True
    stderr: bool = True

    # submission file contents
    docker_image: str = Field(...)
    executable: str = Field(...)
    arguments: str = Field(...)
    log_prefix: Optional[pathlib.Path] = None
    memory: str = "2G"
    input_files: Optional[str]
    queue: str = "1"

    @validator("path")
    def _expand_path(  # pylint: disable=no-self-use,no-self-argument
        cls, path: pathlib.Path  # noqa
    ) -> pathlib.Path:
        return path.expanduser().resolve()

    @validator("log_prefix", always=True)
    def _set_default_log_prefix(  # pylint: disable=no-self-use,no-self-argument
        cls, log_prefix: Optional[pathlib.Path], values: Dict[str, Any]  # noqa
    ) -> pathlib.Path:
        # If `log_prefix` is omitted, we  set its value to `HTCondorJob.path`
        if log_prefix is None:
            log_prefix = values["path"].parent / values["path"].stem
        return log_prefix

    def __str__(self) -> str:
        return TEMPLATE.render(**self.dict())


def save_job(job: HTCondorJob) -> None:
    """ Write the job description file to disk """
    job.path.parent.mkdir(exist_ok=True, parents=True)
    job.path.write_text(str(job))


def submit_job(
    job: HTCondorJob,
) -> subprocess.CompletedProcess:  # type: ignore  # pylint: disable=unsubscriptable-object
    """ Submit the job description to the HTCondor scheduler """
    cmd = f"sudo -u {job.proc_user} condor_submit {job.path.as_posix()}"
    proc = subprocess.run(shlex.split(cmd), check=True, capture_output=True, text=True)
    return proc
