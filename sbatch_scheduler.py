#!/usr/bin/env python3
"""
Snakemake SLURM scheduler script
"""

import os
import sys

from snakemake.utils import read_job_properties


def main(args):
    # Get job information
    dependencies = set(args[1:-1])
    jobscript = str(args[1])
    job_properties = read_job_properties(jobscript)
    job_resources = job_properties["resources"]
    name = str(job_properties['rule'])
    threads = str(job_properties.get("threads", "1"))
    mem = str(job_resources.get("mem", "10G"))

    cmd = ['sbatch']

    # Construct sbatch command
    if dependencies:
        cmd.append("--dependency")
        cmd.append("afterok:" + ",".join(dependencies))

    cmd = cmd + [
        "--tasks-per-node", threads, "--mem", mem, "--job-name", name, jobscript
    ]

    # Submit job
    print(
        '\n'.join([
            "Submit job with parameters:", "name : " + name,
            "threads : " + threads, "mem : " + mem
        ]),
        file=sys.stderr)
    os.system(' '.join(cmd))


if __name__ == "__main__":
    main(sys.argv)
