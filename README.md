# htclib

A small library that helps with creating/running HTCondor jobs on JEODPP

## Installation

```
poetry add git+https://github.com/pmav99/htclib.git
```

## Usage

You first create an `HTCondorJob` object. The string representation of the object is a string with
the contents of the job description file.

``` python
import htclib

job = htclib.HTCondorJob(
     path="~/calculate_stuff_1.job",
     proc_user="fooproc",
     memory="5G",
     docker_image="my_image:latest",
     executable="usr/bin/cdo",
     arguments="-info /path/to/netcdf/file.nc",
)

print(job)
```

Prints:
```
universe                = docker
docker_image            = my_image:latest
executable              = usr/bin/cdo
arguments               = -info /path/to/netcdf/file.nc
output                  = /home/feanor/calculate_stuff_1.$(ClusterId).out
error                   = /home/feanor/calculate_stuff_1.$(ClusterId).err
log                     = /home/feanor/calculate_stuff_1.$(ClusterId).log
request_memory          = 5G
should_transfer_files   = YES
when_to_transfer_output = ON_EXIT
queue 1
```


### Real world example

Let's assume that we want to calculate the md5sum of hundreds of huge NetCDF files stored on EOS

```python
import htcilb

# Define the common options for your jobs
COMMON_OPTIONS = dict(
    proc_user="fooproc",
    image="my_image:latest",
    executable="/usr/bin/md5sum",
    memory="5G",
)

# Define a function that takes the variable options (e.g. the file that you want to process)
# of your jobs and returns an `HTCondorJob` object
def create_job(path, arguments):
    # path is the location where you want to save the job description file
    # arguments is the arguments that you want to pass to the `executable` of the job
    job = htclib.HTCondorJob(path=path, arguments=arguments, **COMMON_OPTIONS)
    return job

# This is where we want to store the job description files
jobs_directory = pathlib.Path("/eos/my_jobs")

# Let's create the job objects
jobs = []
for path in pathlib.Path("/eos/foo/bar/baz/").glob("*.nc"):
    job_path = (jobs_dir / path.name).with_suffix(".job")
    jobs.append(create_job(path=job_path, arguments=path.as_posix())

# Let's store the job description objects to disk
for job in jobs:
    htclib.save_job(job)

# Let's submit the jobs:
for job in jobs:
    proc = htclib.submit_job(job)
    proc.check_returncode()
```
