# get-covid-clade-counts
Create virus clade count data in parquet format (eventually: and save to an S3 Bucket) using functionality from virus-clade-utils.

# Usage

## uv

To do this, you will first need to [install uv](https://github.com/astral-sh/uv?tab=readme-ov-file#installation).

With that prerequisite installed, you can run a command like:

```bash
uv run get_covid_clade_counts.py --as-of=2024-09-23
```
