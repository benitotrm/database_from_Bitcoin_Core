'''Module to run a Data Quality check on the vins parquets'''
import os
import pandas as pd
import dask.array as da
import dask.dataframe as dd
from src.utils.commons import get_current_branch

# Display options for debugging
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def setup_environment():
    """Set up the environment variables and directories."""
    print("\nvins_dq.py started...\n")
    branch_name = get_current_branch()
    print(f"Current branch: {branch_name}")
    env = 'main' if branch_name == 'main' else 'dev'
    vin_dir = os.path.join(os.path.dirname(__file__), f'../../database/vins_{env}')
    transactions_dir = os.path.join(os.path.dirname(__file__), f'../../database/transactions_{env}')
    return vin_dir, transactions_dir

def check_non_matching_vins(vins_df, transactions_df):
    '''Check if there are any non-matching records in the vins_df'''
    vins_df = vins_df['vin_txid'].drop_duplicates()

    # Merge and filter DataFrames
    merged_df = vins_df.to_frame().merge(transactions_df, left_on='vin_txid', right_on='txid', how='left', indicator=True)
    non_matching_df = merged_df[(merged_df['_merge'] == 'left_only') & (merged_df['is_coinbase'] != True)]

    # Check if there are any non-matching records
    has_non_matching_records = da.any(non_matching_df['vin_txid'].notnull()).compute()

    if not has_non_matching_records:
        print("All vin_txid values are either matched with txid or are coinbase transactions. Data is consistent.")
    else:
        print("There are unmatched non-coinbase vin_txid. Further investigation needed.")

def main():
    '''Main process for vins_dq.py'''
    vins_dir, transactions_dir = setup_environment()
    vins_df = dd.read_parquet(vins_dir)
    transactions_df = dd.read_parquet(transactions_dir, columns=['txid', 'is_coinbase'])
    check_non_matching_vins(vins_df, transactions_df)
    print(vins_df.tail(10))

if __name__ == "__main__":
    main()
