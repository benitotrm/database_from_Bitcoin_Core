"""Module to run a Data Quality check on the block.parquets"""

import os
import dask.dataframe as dd

parquet_dir = os.path.join(os.path.dirname(__file__), '../../database/blocks')
blocks_df = dd.read_parquet(parquet_dir)

def count_blocks():
    """Counts the number of blocks"""
    count = blocks_df.shape[0].compute()
    print("Count of blocks:", count)

def check_for_duplicate_blocks():
    duplicate_blocks = blocks_df.groupby('block_hash').size().reset_index()
    duplicate_blocks = duplicate_blocks.rename(columns={0: 'count'})
    duplicate_blocks = duplicate_blocks[duplicate_blocks['count'] > 1]

    if duplicate_blocks.shape[0].compute() == 0:
        print("No duplicate blocks found.")
    else:
        print("Duplicate blocks found:")
        print(duplicate_blocks.compute())

def test_blocks_table():
    # Test for consecutive height
    sorted_blocks = blocks_df.sort_values('height')
    heights = sorted_blocks['height'].compute()
    
    if not heights.is_monotonic_increasing or (heights.diff().fillna(0).max() > 1):
        print("Block height not consecutive.")
        return False

    # Test for non-empty hash, time, and tx_count
    required_columns = ['block_hash', 'time', 'tx_count']
    missing_columns = [col for col in required_columns if col not in blocks_df.columns]
    
    if missing_columns:
        print(f"Missing columns in DataFrame: {missing_columns}")
        return False

    null_counts = blocks_df[required_columns].isnull().sum().compute()
    if null_counts.sum() > 0:
        print("Found rows with NULL hash, time, or tx_count.")
        return False

    # Test min and max tx_count
    min_tx_count = blocks_df['tx_count'].min().compute()
    avg_tx_count = blocks_df['tx_count'].mean().compute()
    max_tx_count = blocks_df['tx_count'].max().compute()
    print(f"Min tx_count: {min_tx_count}, Avg tx_count: {avg_tx_count}, Max tx_count: {max_tx_count}")

    print("All tests passed.")
    return True

def query_blocks():
    print(blocks_df.head(10))

if __name__ == "__main__":
    count_blocks()
    check_for_duplicate_blocks()
    TEST_RESULT = test_blocks_table()
    query_blocks()
