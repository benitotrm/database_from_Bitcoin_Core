"""Module to run a Data Quality check on the block.parquets"""
import os
import dask
import pandas as pd
import dask.dataframe as dd
from src.utils.commons import get_current_branch

print("Dask version:", dask.__version__)

# Set display options to avoid truncation
pd.set_option('display.max_colwidth', None)  # Show full content in columns
pd.set_option('display.max_rows', None)      # Display all rows without truncation
pd.set_option('display.max_columns', None)   # Display all columns without truncation

def setup_environment():
    """Set up the environment and print initial information."""
    print("\nblocks_dq.py started...\n")
    branch_name = get_current_branch()
    print(f"Current branch: {branch_name}")
    return 'main' if branch_name == 'main' else 'dev'

def load_data(env):
    """Load the blocks data from parquet files."""
    blocks_dir = os.path.join(os.path.dirname(__file__), f'../../database/blocks_{env}')
    return dd.read_parquet(blocks_dir)

def count_blocks(blocks_df):
    """Count and print the number of blocks."""
    count = blocks_df.shape[0].compute()
    print("Count of blocks:", count)

def check_for_duplicate_blocks(blocks_df):
    """Check for and print any duplicate blocks."""
    duplicate_blocks = blocks_df.groupby('block_hash').size().reset_index().rename(columns={0: 'count'})
    duplicates = duplicate_blocks[duplicate_blocks['count'] > 1].compute()
    
    if duplicates.empty:
        print("No duplicate blocks found.")
    else:
        print("Duplicate blocks found:")
        print(duplicates)

def test_blocks_table(blocks_df):
    """Run a series of tests to ensure data integrity."""
    all_passed = True
    
    # No need to set the index, just access the current index
    heights = blocks_df.index.compute()

    # Test if the heights are consecutive
    if not heights.is_monotonic_increasing or (heights.to_series().diff().fillna(0).max() > 1):
        print("Block height not consecutive.")
        all_passed = False

    # Test for non-empty block_hash, time, and tx_count
    required_columns = ['block_hash', 'time', 'tx_count']
    missing_columns = [col for col in required_columns if col not in blocks_df.columns]
    if missing_columns:
        print(f"Missing columns in DataFrame: {missing_columns}")
        all_passed = False

    null_counts = blocks_df[required_columns].isnull().sum().compute()
    if null_counts.sum() > 0:
        print("Found rows with NULL hash, time, or tx_count.")
        all_passed = False

    # Test min, avg, and max tx_count
    tx_stats = {
        'min': blocks_df['tx_count'].min().compute(),
        'mean': blocks_df['tx_count'].mean().compute(),
        'max': blocks_df['tx_count'].max().compute()
    }
    print(f"Min tx_count: {tx_stats['min']}, Avg tx_count: {tx_stats['mean']:.2f}, Max tx_count: {tx_stats['max']}")

    if all_passed:
        print("All tests passed.")
    return all_passed

def query_blocks(blocks_df):
    """Generate and print a sample of the block data."""
    print(blocks_df.tail(10))

def main():
    """Main function to run all data quality checks."""
    env = setup_environment()
    blocks_df = load_data(env)
    
    count_blocks(blocks_df)
    check_for_duplicate_blocks(blocks_df)
    test_result = test_blocks_table(blocks_df)
    query_blocks(blocks_df)
    
    if not test_result:
        print("Some data quality checks failed.")

if __name__ == "__main__":
    main()