'''Module to run a Data Quality check on the transactions parquets'''
import os
import pandas as pd
import dask.config
import dask.dataframe as dd
from dask.distributed import Client
from src.utils.commons import get_current_branch

# Display options for debugging
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def setup_environment():
    """Set up the environment variables and directories."""
    print("\ntransactions_dq.py started...\n")
    branch_name = get_current_branch()
    print(f"Current branch: {branch_name}")
    env = 'main' if branch_name == 'main' else 'dev'
    transactions_dir = os.path.join(os.path.dirname(__file__), f'../../database/transactions_{env}')
    return transactions_dir

def compute_statistics(transactions_df):
    """Compute basic statistics from the transactions dataframe with exact counts."""

    # Use nunique for 'block_hash' and 'txid' to get exact counts
    unique_hashes = transactions_df['block_hash'].nunique()
    unique_tx = transactions_df['txid'].nunique()

    # Compute null counts and 'is_coinbase' sum per partition
    null_txid_count = transactions_df['txid'].isnull().sum()
    null_block_hash_count = transactions_df['block_hash'].isnull().sum()
    coinbase_tx_count = transactions_df['is_coinbase'].sum()

    # Compute exact unique heights from the index
    unique_heights = transactions_df.index.nunique()

    # Aggregate the results efficiently to avoid memory overload
    results = dd.compute(
        unique_heights, unique_hashes, unique_tx, 
        null_txid_count, null_block_hash_count, coinbase_tx_count
    )

    return results

def check_duplicates(txid_series):
    """Check for duplicate txids and report the corresponding blocks."""
    
    # Group by 'height' (index) and 'txid' (column)
    grouped = txid_series.groupby(['height', 'txid']).size().compute()

    # Filter to only include txids that appear more than once within the same height
    duplicates = grouped[grouped > 1].reset_index()

    num_duplicates = len(duplicates)
    if num_duplicates > 0:
        print(f"\nFound {num_duplicates} duplicate txids in the dataset.")
        # Fetch a small sample of duplicates to display
        sample_duplicates = duplicates[['height', 'txid']].head(10)
        print("Sample of duplicate transactions:")
        print(sample_duplicates)
    else:
        print("\nNo duplicates found.")

def main():
    '''Main process for transactions_dq.py'''
    Client()
    with dask.config.set({'dataframe.shuffle.method': 'tasks'}):
        transactions_dir = setup_environment()
        transactions_df = dd.read_parquet(transactions_dir)
        transactions_df = transactions_df.repartition(npartitions=1000)
        
        # Call the optimized compute_statistics function
        stats = compute_statistics(transactions_df)

        # Unpack the results
        (unique_heights, unique_hashes, unique_tx, 
         null_txid_count, null_block_hash_count, coinbase_tx_count) = stats

        # Use the statistics as needed
        print(f"Unique Heights: {unique_heights}")
        print(f"Unique Block Hashes: {unique_hashes}")
        print(f"Unique Transactions: {unique_tx}")
        print(f"Null txid Count: {null_txid_count}")
        print(f"Null block_hash Count: {null_block_hash_count}")
        print(f"Coinbase Transactions Count: {coinbase_tx_count}")

        # txid_series = transactions_df[['txid']]
        # check_duplicates(txid_series)

        print("\nLast few lines of the transactions DataFrame:")
        print(transactions_df.tail(10))

if __name__ == "__main__":
    main()