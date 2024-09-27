"""
Create a parquet file with clade counts by date and location and save it to an S3 bucket.

This script wraps the virus-clade-utils package, which generates the clade counts using the
GenBank-based Sars-CoV-2 sequence metadata from Nextstrain.
https://github.com/reichlab/virus-clade-utils

The script is scheduled to run every Monday, for use in the modeling round that will open
on the following Wednesday.

To run the script manually:
1. Install uv on your machine: https://docs.astral.sh/uv/getting-started/installation/
2. From the root of this repo: uv run get_covid_clade_counts.py --as-of=YYYY-MM-DD
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "virus_clade_utils@git+https://github.com/reichlab/virus-clade-utils/",
# ]
# ///

import click
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

from virus_clade_utils.util.config import Config
from virus_clade_utils.util.sequence import (
    download_covid_genome_metadata,
    filter_covid_genome_metadata,
    get_clade_counts,
    get_covid_genome_metadata,
)
from virus_clade_utils.util.session import get_session

data_dir = "./data"
os.makedirs(data_dir, exist_ok=True)

# Log to stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s -  %(levelname)s - %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@click.command()
@click.option(
    "--as-of",
    type=str,
    required=False,
    help="Get counts based on the last available Nextstrain sequence metadata on or prior to this date (YYYY-MM-DD)",
)
def main(as_of: str = None):
    """Get clade counts and save to S3 bucket."""

    # sequence_date and reference_tree_as_of don't actually do anything here
    # but are required for creating a Config instance (this is what needs a refactor)
    # (the reason we're instantiating a config object is to get config.date_path)
    sequence_date = reference_tree_as_of = datetime.now()
    config = Config(sequence_date, reference_tree_as_of)

    bucket = Config.nextstrain_ncov_bucket
    key = Config.nextstrain_genome_metadata_key

    session = get_session()
    metadata = download_covid_genome_metadata(
        session=session,
        bucket=bucket,
        key=key,
        data_path=Path('metadata'),
        as_of=as_of,
        use_existing=True
    )
    logger.info("get_covid_genome_metadata")
    lf_metadata = get_covid_genome_metadata(metadata)
    logger.info("filter_covid_genome_metadata")
    lf_metadata_filtered = filter_covid_genome_metadata(lf_metadata)
    logger.info("get_clade_counts")
    counts = get_clade_counts(lf_metadata_filtered)
    output_file = f"data/{as_of}_covid_clade_counts.parquet"
    logger.info("collect")
    cc = counts.collect()
    logger.info("write_parquet")
    cc.write_parquet(output_file)

    logger.info(f"Clade outputs saved: {output_file}")


if __name__ == "__main__":
    main()