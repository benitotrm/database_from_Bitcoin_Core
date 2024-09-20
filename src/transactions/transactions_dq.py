'''Module to run a Data Quality check on the transactions parquets'''
import os
import pandas as pd
import dask.dataframe as dd
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
    """Compute basic statistics from the transactions dataframe."""
    unique_hashes = transactions_df['block_hash'].nunique()
    unique_tx = transactions_df['txid'].nunique()
    null_txid_count = transactions_df['txid'].isnull().sum()
    null_block_hash_count = transactions_df['block_hash'].isnull().sum()
    coinbase_tx_count = transactions_df['is_coinbase'].sum()

    return dd.compute(unique_hashes, unique_tx, null_txid_count, null_block_hash_count, coinbase_tx_count)

def check_coinbase_consistency(transactions_df, all_blocks):
    """Check if there is at least one coinbase transaction per block."""
    coinbase_blocks = transactions_df[transactions_df['is_coinbase']].index.unique().compute()
    missing_coinbase_blocks = set(all_blocks) - set(coinbase_blocks)

    if missing_coinbase_blocks:
        print(f"Discrepancy Alert: Missing coinbase transactions for blocks: {sorted(missing_coinbase_blocks)}")
    else:
        print("All blocks have at least one coinbase transaction.")

# def check_duplicates(transactions_df):
#     """Check for duplicate transactions based on txid, and report the corresponding heights."""
#     # Group by 'txid' and count the occurrences
#     duplicate_groups = transactions_df.groupby('txid').size().compute()
    
#     # Filter to get txids that have more than one occurrence
#     duplicates = duplicate_groups[duplicate_groups > 1]
    
#     if not duplicates.empty:
#         # Fetch the rows with the duplicate txids and display the corresponding heights
#         duplicate_txids = duplicates.index.tolist()
#         duplicate_rows = transactions_df[transactions_df['txid'].isin(duplicate_txids)].compute()
        
#         # Print the duplicates with their associated heights
#         print(f"\nFound {len(duplicate_rows)} duplicate transactions in the following blocks:")
#         print(duplicate_rows[['txid', 'block_hash', 'is_coinbase']])
#     else:
#         print("\nNo duplicates found.")

# def check_block_continuity(transactions_df, unique_hashes, all_blocks):
#     """Check if block heights are continuous and print missing blocks if any."""
#     min_block_height = transactions_df.index.min().compute()
#     max_block_height = transactions_df.index.max().compute()
#     expected_blocks = set(range(min_block_height, max_block_height + 1))
#     missing_blocks = expected_blocks - set(all_blocks)
#     total_expected_blocks = max_block_height - min_block_height + 1

#     if total_expected_blocks != unique_hashes or missing_blocks:
#         print(f"\nBlock Heights are not continuous. Expected {total_expected_blocks} blocks, found {unique_hashes}.")
#         print(f"Missing blocks: {sorted(missing_blocks)}")
#     else:
#         print(f"\nBlock Heights are continuous from {min_block_height} to {max_block_height}.")

def main():
    '''Main process for transactions_dq.py'''
    transactions_dir = setup_environment()
    transactions_df = dd.read_parquet(transactions_dir)

    stats = compute_statistics(transactions_df)
    unique_hashes, unique_tx, null_txid_count, null_block_hash_count, coinbase_tx_count = stats

    print(f"\nTotal Number of Blocks: {unique_hashes}")
    print(f"Total Number of Transactions: {len(transactions_df)}")
    print(f"Number of Unique Transactions: {unique_tx}")
    print(f"Number of Transactions with Missing txid: {null_txid_count}")
    print(f"Number of Transactions with Missing block_hash: {null_block_hash_count}")
    print(f"Number of Coinbase Transactions: {coinbase_tx_count}")

    all_blocks = transactions_df.index.unique().compute()
    check_coinbase_consistency(transactions_df, all_blocks)
    # check_duplicates(transactions_df)
    # check_block_continuity(transactions_df, unique_hashes, all_blocks)

    print("\nLast few lines of the transactions DataFrame:")
    print(transactions_df.tail(10))

if __name__ == "__main__":
    main()