# snakemake_sbatch_scheduler
## A simple snakemake SLURM scheduler with immediate-submit support

**Usage:**

```bash
  snakemake --cluster snakemake_sbatch_scheduler.py -j 499 --immediate-submit
```

Set -j to the max value allowed by your system administrator.

Job memory and thread requirements are read from the Snakefile. The cluster
config file is ignored.

By default jobs request 10 gigabytes of memory and 1 thread.
