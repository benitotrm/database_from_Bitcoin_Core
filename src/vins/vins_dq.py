'''Module to run a Data Quality check on the vins parquets in batches'''
import os
import polars as pl
from src.utils.commons import get_current_branch

BATCH_SIZE = 5000

def setup_environment():
    """Set up the environment variables and directories."""
    print("\nvins_dq.py started...\n")
    branch_name = get_current_branch()
    print(f"Current branch: {branch_name}")
    env = 'main' if branch_name == 'main' else 'dev'
    vin_dir = os.path.join(os.path.dirname(__file__), f'../../database/vins_{env}')
    transactions_dir = os.path.join(os.path.dirname(__file__), f'../../database/transactions_{env}')
    return vin_dir, transactions_dir

def process_batches(vins_dir, transactions_dir):
    '''Process vins and transactions data in batches'''
    # Load the full transactions DataFrame (only relevant columns)
    transactions_df = pl.scan_parquet(transactions_dir).select(['txid', 'is_coinbase', 'height'])

    # Get min and max height from vins data
    vins_df = pl.scan_parquet(vins_dir)
    min_height = vins_df.select(pl.col('height').min()).collect().item()
    max_height = vins_df.select(pl.col('height').max()).collect().item()

    # Process in batches of BATCH_SIZE
    for start_height in range(min_height, max_height + 1, BATCH_SIZE):
        end_height = start_height + BATCH_SIZE - 1
        print(f"Processing batch: {start_height} to {end_height}")

        # Filter vins DataFrame for the current batch
        batch_vins_df = vins_df.filter((pl.col('height') >= start_height) & (pl.col('height') <= end_height)).collect()

        # Ensure that the filtered batch DataFrame is not empty
        if batch_vins_df.is_empty():
            print(f"***NO RECORDS FOUND for batch: {start_height} to {end_height}")
            continue

        # Crop transactions DataFrame up to end_height
        cropped_transactions_df = transactions_df.filter(pl.col('height') <= end_height).collect()

        # Perform the data quality check for the current batch
        vin_txids = batch_vins_df.select('vin_txid').to_series().to_list()
        batch_transactions_df = cropped_transactions_df.filter(pl.col('txid').is_in(vin_txids))

        # Ensure that the filtered transactions DataFrame is not empty
        if batch_transactions_df.is_empty():
            print(f"***NO MATCHING transactions found for batch: {start_height} to {end_height}")
            continue

        print(f"COMPLETE match between vins and transactions.")

def main():
    '''Main process for vins_dq.py'''
    vins_dir, transactions_dir = setup_environment()
    process_batches(vins_dir, transactions_dir)

if __name__ == "__main__":
    main()
