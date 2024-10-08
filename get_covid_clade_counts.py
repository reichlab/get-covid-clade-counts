"""
Create a parquet file with clade counts by date and location and save it to an S3 bucket.

This script wraps the virus-clade-utils package, which generates the clade counts using the
GenBank-based Sars-CoV-2 sequence metadata from Nextstrain.
https://github.com/reichlab/cladetime

The script is scheduled to run every Monday, for use in the modeling round that will open
on the following Wednesday.

To run the script manually:
1. Install uv on your machine: https://docs.astral.sh/uv/getting-started/installation/
2. From the root of this repo: uv run get_covid_clade_counts.py --as-of=YYYY-MM-DD
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "click",
#   "virus_clade_utils@git+https://github.com/reichlab/virus-clade-utils"
# ]
# ///

import click
import os
import logging

from virus_clade_utils.cladetime import CladeTime  # type: ignore
from virus_clade_utils.util.sequence import filter_covid_genome_metadata, get_clade_counts  # type: ignore

# Log to stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s -  %(levelname)s - %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


data_dir = "./data"
os.makedirs(data_dir, exist_ok=True)


@click.command()
@click.option(
    "--as-of",
    type=str,
    required=False,
    help="Get counts based on the last available Nextstrain sequence metadata on or prior to this date (YYYY-MM-DD)",
)
def main(as_of: str | None = None):
    """Get clade counts and save to S3 bucket."""

    # Instantiate CladeTime object with specified as_of date
    ct = CladeTime(sequence_as_of=as_of)
    logger.info({
        "msg": f"CladeTime object created with sequence of_of date = {ct.sequence_as_of}",
        "nextstrain_metadata_url": ct.url_sequence_metadata,
    })

    # get Polars LazyFrame to Nextstrain sequence metadata
    lf_metadata = ct.sequence_metadata

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