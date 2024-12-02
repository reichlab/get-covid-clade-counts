"""
Create a parquet file with clade counts by date and location and save it to an S3 bucket.

This script wraps the cladetime package, which generates the clade counts using the
GenBank-based Sars-CoV-2 sequence metadata from Nextstrain.
https://github.com/reichlab/cladetime

The script is scheduled to run every Monday, for use in the modeling round that will open
on the following Wednesday.

To run the script manually:
1. Install uv on your machine: https://docs.astral.sh/uv/getting-started/installation/
2. From the root of this repo: uv run get_covid_clade_counts.py --as-of=YYYY-MM-DD
"""

# /// script
# requires-python = "==3.12"
# dependencies = [
#   "click",
#   "cladetime@git+https://github.com/reichlab/cladetime@elr/use_metadata_url"
# ]
# ///

import click
from pathlib import Path
import logging
from datetime import datetime, timedelta, timezone

from cladetime import CladeTime, sequence  # type: ignore

# Log to stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s -  %(levelname)s - %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# set up data directory
data_dir = Path("./data")
data_dir.mkdir(exist_ok=True)

@click.command()
@click.option(
    "--as-of",
    type=str,
    required=False,
    help="Get counts based on the last available Nextstrain sequence metadata on or prior to this date (YYYY-MM-DD)",
)
def main(as_of: str | None = None):
    """Get clade counts and save to S3 bucket."""
    utc_now = datetime.now(tz=timezone.utc)
    utc_now_date = utc_now.date()
    if as_of is None or as_of == str(utc_now_date):
        # as_of not provided or is today
        as_of_datetime = utc_now
        as_of = str(utc_now_date)
    elif as_of > str(utc_now_date):
        raise ValueError('as_of is in the future!')
    else:
        # as_of is not today
        as_of_datetime = datetime.strptime(as_of, "%Y-%m-%d") \
            + timedelta(hours = 23, minutes = 59, seconds = 59)

    # Instantiate CladeTime object
    ct = CladeTime(sequence_as_of=as_of_datetime)
    logger.info({
        "msg": f"CladeTime object created with sequence as_of date = {ct.sequence_as_of}",
        "nextstrain_metadata_url": ct.url_sequence_metadata,
    })

    # get Polars LazyFrame to Nextstrain sequence metadata
    lf_metadata = ct.sequence_metadata

    logger.info("filter_metadata")
    lf_metadata_filtered = sequence.filter_metadata(lf_metadata)
    logger.info("get_clade_counts")
    counts = sequence.summarize_clades(lf_metadata_filtered, group_by=["clade", "date", "location"])


    output_file = f"data/{as_of}_covid_clade_counts.parquet"
    logger.info("collecting clade counts")
    cc = counts.collect(streaming=True)
    logger.info("write_parquet")
    cc.write_parquet(output_file)

    logger.info(f"Clade outputs saved: {output_file}")


if __name__ == "__main__":
    main()