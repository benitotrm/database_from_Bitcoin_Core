'''Module to run a Data Quality check on the transactions parquets'''
import os
import pandas as pd
import dask.dataframe as dd
from src.utils.commons import get_current_branch

# Set display options to avoid truncation
pd.set_option('display.max_colwidth', None)  # Show full content in columns
pd.set_option('display.max_rows', None)      # Display all rows without truncation
pd.set_option('display.max_columns', None)   # Display all columns without truncation

def setup_environment():
    """Set up the environment variables and directories."""
    print("\ntransactions_dq.py started...\n")
    branch_name = get_current_branch()
    print(f"Current branch: {branch_name}")
    env = 'main' if branch_name == 'main' else 'dev'
    blocks_dir = os.path.join(os.path.dirname(__file__), f'../../database/blocks_{env}')
    transactions_dir = os.path.join(os.path.dirname(__file__), f'../../database/transactions_{env}')
    return env, blocks_dir, transactions_dir

def load_data(blocks_dir, transactions_dir):
    """Load and prepare the dataframes."""
    blocks_df = dd.read_parquet(blocks_dir)
    transactions_df = dd.read_parquet(transactions_dir)
    transactions_df = transactions_df.repartition(npartitions=500)
    return blocks_df, transactions_df

def compute_statistics(transactions_df):
    """Compute basic statistics from the transactions dataframe."""
    unique_hashes = transactions_df['block_hash'].nunique()
    unique_tx = transactions_df['txid'].nunique()
    null_txid_count = transactions_df['txid'].isnull().sum()
    null_block_hash_count = transactions_df['block_hash'].isnull().sum()
    coinbase_tx_count = transactions_df['is_coinbase'].sum()

    return dd.compute(unique_hashes, unique_tx, null_txid_count, null_block_hash_count, coinbase_tx_count)

def check_coinbase_consistency(coinbase_tx_count, unique_hashes):
    """Check if the number of coinbase transactions matches the number of blocks."""
    if coinbase_tx_count != unique_hashes:
        print(f"Discrepancy Alert: Number of coinbase transactions ({coinbase_tx_count}) does not match the number of blocks ({unique_hashes}).")
    else:
        print("Coinbase transaction count matches the number of blocks.")

def check_duplicates(transactions_df):
    """Check for duplicate transactions using value_counts across partitions."""
    
    # Get the count of 'txid' occurrences across partitions
    txid_counts = transactions_df['txid'].value_counts()

    # Find txids that occur more than once (duplicates)
    duplicate_txids = txid_counts[txid_counts > 1].index

    # Use a Dask DataFrame to filter for duplicates without computing the entire DataFrame
    duplicates_df = transactions_df[transactions_df['txid'].isin(duplicate_txids)]

    # Compute and print the sample of duplicates if any are found
    if not duplicates_df.is_empty().compute():
        print("\nSample of Duplicate Transactions with Block Hashes:")
        pd.set_option('display.max_columns', None)  # Ensure full columns are displayed
        pd.set_option('display.max_colwidth', None)  # Ensure full column width is displayed
        print(duplicates_df[['txid', 'block_hash', 'is_coinbase']].drop_duplicates().compute())
    else:
        print("\nNo duplicates found.")

def check_block_continuity(joined_df, unique_hashes):
    """Check if block heights are continuous and print missing blocks if any."""
    min_block_height = joined_df['height'].min().compute()
    max_block_height = joined_df['height'].max().compute()
    expected_blocks = set(range(min_block_height, max_block_height + 1))
    actual_blocks = set(joined_df['height'].unique().compute())
    total_expected_blocks = max_block_height - min_block_height + 1
    missing_blocks = expected_blocks - actual_blocks

    if total_expected_blocks != unique_hashes or missing_blocks:
        print(f"\nBlock Heights are not continuous. Expected {total_expected_blocks} blocks, found {unique_hashes}.")
        print(f"Missing blocks: {sorted(missing_blocks)}")  
    else:
        print(f"\nBlock Heights are continuous from {min_block_height} to {max_block_height}.")

def main():
    '''Main process for transactions_dq.py'''
    _, blocks_dir, transactions_dir = setup_environment()
    blocks_df, transactions_df = load_data(blocks_dir, transactions_dir)

    stats = compute_statistics(transactions_df)
    print(f"\nTotal Number of Blocks: {stats[0]}")
    print(f"Total Number of Transactions: {len(transactions_df)}")
    print(f"Number of Unique Transactions: {stats[1]}")
    print(f"Number of Transactions with Missing txid: {stats[2]}")
    print(f"Number of Transactions with Missing block_hash: {stats[3]}")
    print(f"Number of Coinbase Transactions: {stats[4]}")

    number_of_coinbase_transactions = stats[4]
    total_number_of_blocks = stats[0]
    check_coinbase_consistency(number_of_coinbase_transactions, total_number_of_blocks)

    check_duplicates(transactions_df)

    joined_df = transactions_df.merge(blocks_df, on='block_hash', how='inner')
    check_block_continuity(joined_df, total_number_of_blocks)

    print("\nFirst few lines of the transactions DataFrame:")
    print(transactions_df.head(10))

if __name__ == "__main__":
    main()

