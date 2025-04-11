# get-covid-clade-counts
This bucket contains code to create virus clade count data in parquet format and save that data in a S3 Bucket using functionality from cladetime. Every Monday morning this repo runs a workflow that runs get_covid_clade_counts.py to extract a snapshot of the current Nextstrain data using cladetime, saves that data as a parquet file, and puts it in a s3 bucket. The infomation saved includes: the number of observations seen, the location of the observations, the clades of the observations, and the dates of the observations.

# Usage

## uv

To run this work flow manually, you will first need to [install uv](https://github.com/astral-sh/uv?tab=readme-ov-file#installation).

With that prerequisite installed, you can run a command like:

```bash
uv run get_covid_clade_counts.py --as-of=2024-09-23
```
## Accessing the data
The data stored in the s3 bucket can be accessed using commands like the following R command
```r
arrow::read_parquet(paste("https://covid-clade-counts.s3.amazonaws.com/",
                             as.Date(target_date), "_covid_clade_counts.parquet", sep = ""))
```
where 
```r
target_date
```
 is a Monday date that you want to access.
