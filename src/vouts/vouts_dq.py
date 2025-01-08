'''Module to run a Data Quality check on the vouts parquets in batches'''
import os
import polars as pl
from src.utils.commons import get_current_branch

# Configure Polars to display full string lengths and wider tables
pl.Config.set_fmt_str_lengths(100)  # Set max string length to 100 characters
pl.Config.set_tbl_width_chars(200)  # Adjust table width to 200 characters

BATCH_SIZE = 100000

def setup_environment():
    """Set up the environment variables and directories."""
    print("\nvouts_dq.py started...\n")
    branch_name = get_current_branch()
    print(f"Current branch: {branch_name}")
    env = 'main' if branch_name == 'main' else 'dev'
    vouts_dir = os.path.join(os.path.dirname(__file__), f'../../database/vouts_{env}')
    return vouts_dir

def process_batches(vouts_dir):
    '''Process vouts data in batches'''
    # Get min and max height from vouts data
    vouts_df = pl.scan_parquet(vouts_dir)
    min_height = vouts_df.select(pl.col('height').min()).collect().item()
    max_height = vouts_df.select(pl.col('height').max()).collect().item()

    # Process in batches of BATCH_SIZE
    for start_height in range(min_height, max_height + 1, BATCH_SIZE):
        end_height = start_height + BATCH_SIZE - 1
        print(f"Processing batch: {start_height} to {end_height}")

        # Filter vouts DataFrame for the current batch
        batch_vouts_df = vouts_df.filter((pl.col('height') >= start_height) & (pl.col('height') <= end_height)).collect()

        # Ensure that the filtered batch DataFrame is not empty
        if batch_vouts_df.is_empty():
            print(f"No records found for vouts in batch: {start_height} to {end_height}")
            continue

        # Perform the data quality check for the current batch
        dq_check(batch_vouts_df)

def dq_check(batch_vouts_df):
    '''Data Quality Check to ensure max vout and n values match for each txid and analyze address presence'''
    print("Performing Data Quality Check...")

    # Check address presence
    address_presence = batch_vouts_df.select(
        pl.col("txid"),
        pl.col("n"),
        (pl.col("addresses") != "").alias("has_address")
    ).group_by(["txid", "n", "has_address"]).agg(pl.col("txid").count().alias("txid_count"))

    summary = address_presence.group_by("has_address").agg(pl.col("txid_count").sum().alias("total"))
    has_address = summary.filter(pl.col("has_address") == True)["total"].sum()
    no_address = summary.filter(pl.col("has_address") == False)["total"].sum()

    print(f"Address presence summary: {has_address} txid-n combinations have addresses, {no_address} txid-n combinations do not.")
    print(address_presence)

    '''Data Quality Check to ensure max vout and n values match for each txid'''
    print("Performing Data Quality Check...")

    # Count the number of rows per txid
    vouts_count_n = batch_vouts_df.group_by("txid").agg(pl.col("n").count().alias("count_n"))

    # Get the max value of 'n' per txid
    vouts_max_n = batch_vouts_df.group_by("txid").agg(pl.col("n").max().alias("max_n"))

    # Join the two DataFrames on 'txid'
    dq_result = vouts_count_n.join(vouts_max_n, on="txid", how="inner")

    # Check if 'max_n + 1' matches 'count_n' for all records
    mismatched = dq_result.filter(pl.col("count_n") != pl.col("max_n") + 1)

    if mismatched.is_empty():
        print("Data Quality Check Passed: All records match.")
    else:
        print("Data Quality Check Failed: Mismatched records found.")
        print(mismatched)

def main():
    '''Main process for vouts_dq.py'''
    vouts_dir = setup_environment()
    process_batches(vouts_dir)

if __name__ == "__main__":
    main()
