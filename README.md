# snakemake_sbatch_scheduler
## A simple snakemake SLURM scheduler with immediate-submit support

**Usage:**

```bash
  snakemake --cluster snakemake_sbatch_scheduler.py -j 499 --immediate-submit
```

Set -j to the max value allowed by your system administrator.
