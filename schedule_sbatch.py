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
from time import sleep

from plumbum import ProcessExecutionError
from plumbum.cmd import (
    grep,
    squeue,
)
from snakemake.utils import read_job_properties


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

        cmd += ['--job-name', self.jobname,
                '--tasks-per-node', self.threads,
                '--mem', self.mem,
                self.jobscript]

        return cmd

    def print_summary(self):
        '''Print a summary of the submitted job to stderr'''
        errprint('Submit job with parameters:')
        errprint('name: ' + self.jobname)
        errprint('threads: ' + self.threads)
        errprint('mem(mb): ' + self.mem)
        errprint('sbatch command: ' + ' '.join(self.command))

    def has_remaining_dependencies(self):
        '''Return True if the job has dependencies'''
        for dependency in self.dependencies:
            try:
                (squeue | grep[dependency])()
                return True
            except ProcessExecutionError:
                pass
        return False

    def submit(self):
        '''Submit job to SLURM'''
        if self.jobname == 'all':
            # If snakemake is passed the immediate-submit parameter, its main
            # process terminates as soon as all jobs are submitted. This
            # masks further updates on job completions/failures. To avoid this
            # we wait until all jobs are complete before submitting 'all' for
            # scheduling.
            while self.has_remaining_dependencies():
                sleep(10)

        self.print_summary()
        sbatch_stdout = Popen(self.command, stdout=PIPE,
                              stderr=STDOUT).communicate()[0]

        # Snakemake expects the job's id to be sent to stdout by the
        # scheduler
        print("%i" % int(sbatch_stdout.split()[-1]), file=sys.stdout)


def errprint(x):
    '''Print to stderr'''
    print(x, file=sys.stderr)


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
