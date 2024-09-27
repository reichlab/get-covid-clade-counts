# get-covid-clade-counts
Create virus clade count data in parquet format (eventually: and save to an S3 Bucket) using functionality from virus-clade-utils.

# Usage

## uv

To do this, you will first need to [install uv](https://github.com/astral-sh/uv?tab=readme-ov-file#installation).

With that prerequisite installed, you can run a command like:

```bash
uv run get_covid_clade_counts.py --as-of=2024-09-23
```

## Docker

Actually none of the stuff below using Docker works!

### To build the Docker image:

    ```bash
    docker build --platform=linux/amd64 -t covid-clade-counts .
    docker build -t covid-clade-counts .
    ```

### To run for a particular as_of date:

    ```bash
    docker run --platform linux/amd64 \
        -v $(pwd)/data:/home/docker-user/data/ \
        covid-clade-counts \
        python get_covid_clade_counts.py --as-of 2024-09-23


    docker run \
        -v $(pwd)/data:/home/docker-user/data/ \
        covid-clade-counts \
        python get_covid_clade_counts.py --as-of 2024-09-23


    docker run -it \
        -v $(pwd)/data:/home/docker-user/data/ \
        covid-clade-counts \
        python get_covid_clade_counts.py --as-of 2024-09-23
    ```
