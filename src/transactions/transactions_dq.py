'''Module to run a Data Quality check on the transactions parquets'''
import os
import dask.dataframe as dd
from src.utils.commons import get_current_branch

branch_name = get_current_branch()
print(f"Current branch: {branch_name}")

ENV = 'main' if branch_name == 'main' else 'dev'

blocks_dir = os.path.join(os.path.dirname(__file__), f'../../database/blocks_{ENV}')
blocks_df = dd.read_parquet(blocks_dir)

transactions_dir = os.path.join(os.path.dirname(__file__), f'../../database/transactions_{ENV}')
transactions_df = dd.read_parquet(transactions_dir)

# Compute essential statistics
unique_hashes = transactions_df['block_hash'].nunique()
unique_tx = transactions_df['txid'].nunique()
null_txid_count = transactions_df['txid'].isnull().sum()
null_block_hash_count = transactions_df['block_hash'].isnull().sum()
coinbase_tx_count = transactions_df['is_coinbase'].sum()

# Compute all together
unique_hashes, unique_tx, null_txid_count, null_block_hash_count, coinbase_tx_count = dd.compute(
    unique_hashes, unique_tx, null_txid_count, null_block_hash_count, coinbase_tx_count)

print(f"\nTotal Number of Blocks: {unique_hashes}")
print(f"Total Number of Transactions: {len(transactions_df)}")
print(f"Number of Unique Transactions: {unique_tx}")
print(f"Number of Transactions with Missing txid: {null_txid_count}")
print(f"Number of Transactions with Missing block_hash: {null_block_hash_count}")
print(f"Number of Coinbase Transactions: {coinbase_tx_count}")

# Check if coinbase transactions match the number of blocks
if coinbase_tx_count != unique_hashes:
    print(f"Discrepancy Alert: Number of coinbase transactions ({coinbase_tx_count}) does not match the number of blocks ({unique_hashes}).")
else:
    print("Coinbase transaction count matches the number of blocks.")

# Check for duplicates only if necessary
if unique_tx < len(transactions_df):
    duplicates_count = transactions_df.groupby('txid').size()
    duplicates = duplicates_count[duplicates_count > 1].compute()
    if len(duplicates) > 0:
        print("\nSample of Duplicate Transactions:")
        print(duplicates.head())

# Join transactions with blocks for continuity check
joined_df = transactions_df.merge(blocks_df, on='block_hash', how='inner')
min_block_height = joined_df['height'].min().compute()
max_block_height = joined_df['height'].max().compute()
total_expected_blocks = max_block_height - min_block_height + 1

# Continuity check
if total_expected_blocks != unique_hashes:
    print(f"\nBlock Heights are not continuous. Expected {total_expected_blocks} blocks, found {unique_hashes}.")
    # Additional checks for missing coinbase transactions and blocks could go here.
else:
    print(f"\nBlock Heights are continuous from {min_block_height} to {max_block_height}.")
