#!/usr/bin/env python3
"""
Snakemake SLURM scheduler script

Usage:
snakemake -j 1000 --debug --immediate-submit --cluster 'snakemake_sbatch.py
{dependencies}'
"""
from subprocess import (
    PIPE,
    Popen,
    STDOUT,
)
import sys

from snakemake.utils import read_job_properties


def errprint(x):
    '''Print to stderr'''
    print(x, file=sys.stderr)


class SnakemakeSbatchScheduler():
    '''Class for scheduling snakemake jobs with SLURM

    All parameters are automatically produced by snakemake

    Parameters
    ----------
    jobscript: str
    Path to a snakemake jobscript
    dependencies: List[str]
    List of SLURM job ids
    '''
    def __init__(self, jobscript, dependencies=None):
        self.jobscript = jobscript
        self.dependencies = dependencies
        job_properties = read_job_properties(jobscript)
        self.jobname = str(job_properties['rule'])
        self.threads = str(job_properties.get('threads', '1'))
        self.mem = str(job_properties['resources'].get('mem_mb', '10000'))
        self.command = self.construct_command()

    def construct_command(self):
        '''Construct the sbatch command from the jobscript'''
        cmd = ['sbatch']

        # Construct sbatch command
        if self.dependencies:
            cmd.append("--dependency")
            cmd.append(','.join(['afterok:%s' % d for d in self.dependencies]))

        cmd += ['--job-name', self.jobname]
        cmd += ['--tasks-per-node', self.threads]
        cmd += ['--mem', self.mem]
        cmd += [self.jobscript]
        return cmd

    def print_summary(self):
        '''Print a summary of the submitted job to stderr'''
        errprint('Submit job with parameters:')
        errprint('name: ' + self.jobname)
        errprint('threads: ' + self.threads)
        errprint('mem(mb): ' + self.mem)
        errprint('sbatch command: ' + ' '.join(self.command))

    def submit(self):
        '''Submit job to SLURM'''
        self.print_summary()
        sbatch_stdout = Popen(self.command, stdout=PIPE,
                              stderr=STDOUT).communicate()[0]

        # Snakemake expects the job's id to be sent to stdout by the scheduler
        print("%i" % int(sbatch_stdout.split()[-1]), file=sys.stdout)


if __name__ == "__main__":
    # Snakemake passes a list of dependencies followed by the jobscript to the
    # scheduler.
    jobscript = sys.argv[-1]

    if len(sys.argv) > 2:
        dependencies = sys.argv[1:-1]
    else:
        dependencies = None

    sbatch = SnakemakeSbatchScheduler(jobscript, dependencies)
    sbatch.submit()
