#!/usr/bin/env python
"""
Snakemake SLURM scheduler script
"""
from __future__ import print_function

import os
from subprocess import (
    PIPE,
    Popen,
    STDOUT,
)
import sys

from snakemake.utils import read_job_properties


def errprint(x):
    print(x, file=sys.stderr)


def main(args):
    # Get job information
    dependencies = set(args[1:-1])
    jobscript = str(args[-1])
    job_properties = read_job_properties(jobscript)
    job_resources = job_properties["resources"]
    name = str(job_properties['rule'])
    threads = str(job_properties.get("threads", "1"))
    mem = str(job_resources.get("mem_mb", "10000"))

    cmd = ['sbatch']

    # Construct sbatch command
    if dependencies:
        cmd.append("--dependency")
        cmd.append("afterok:" + ",".join(dependencies))

    cmd += ["--tasks-per-node", threads, "--mem", mem, "--job-name", name,
            jobscript]

    # Submit job
    errprint('Submit job with parameters:')
    errprint('name: ' + name)
    errprint('threads: ' + threads)
    errprint('mem(mb): ' + mem)
    errprint('sbatch command: ' + cmd)

    sbatch_stdout = Popen(cmd, stdout=PIPE, stderr=STDOUT,
                          shell=True).communicate()[0]

    # Snakemake expects the job's id to be sent to stdout by the scheduler
    print("%i" % int(sbatch_stdout.split()[-1]), file=sys.stdout)


if __name__ == "__main__":
    main(sys.argv)
