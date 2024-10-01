import os
import argparse
import pandas as pd
import pyarrow as pa
import dask.dataframe as dd
from src.api.rpc_client import RPCClient
from src.utils.commons import (get_current_branch, get_max_block_height_on_file, 
                               consolidate_parquet_files, delete_unconsolidated_directory)

def setup_environment():
    """Set up the environment and print initial information."""
    print("\nRunning populate_transactions.py\n")
    branch_name = get_current_branch()
    print(f"Current branch: {branch_name}")
    return 'main' if branch_name == 'main' else 'dev'

def define_schema():
    """Define and return the schema for blocks data."""
    return pa.schema([
        ('height', pa.int32()),  # Non-unique index
        ('txid', pa.string()),
        ('vin_txid', pa.string()),
        ('vout', pa.int32())
    ])

def fetch_vins_data(rpc_client, transactions_with_height):
    """Fetch data for a specific transaction along with height."""
    vin_data = rpc_client.rpc_call_batch("getrawtransaction", [{"txid": str(txid), "verbose": 1} for txid, height in transactions_with_height])

    vin_rows = []
    for (txid, height), response in zip(transactions_with_height, vin_data):
        if response is not None and response.get('result') is not None:
            for vin in response['result']['vin']:
                if 'txid' in vin:
                    vin_txid = vin['txid']
                    vout = int(vin['vout'])
                    vin_rows.append((height, txid, vin_txid, vout))

    return pd.DataFrame(vin_rows, columns=['height', 'txid', 'vin_txid', 'vout'])


def process_vins(start_block, end_block, max_block_height_on_file, env, rpc_client, vins_schema):
    # Constants for batch processing
    BATCH_SIZE = 100
    vin_batch_count = 0

    input_directory = f"database/vins_batches_{env}"
    output_directory = f"database/vins_{env}"

    if not os.path.exists(input_directory):
        os.makedirs(input_directory)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    transactions_dir = os.path.join(os.path.dirname(__file__), f'../../database/transactions_{env}')
    transactions_df = dd.read_parquet(transactions_dir, columns=['txid', 'is_coinbase'])

    if start_block is not None:
        START_BLOCK = start_block
    elif os.path.exists(output_directory) and len(os.listdir(output_directory)) > 0:
        vins_df = dd.read_parquet(output_directory)
        last_processed_height = vins_df.index.max().compute()
        print(f"Last processed height: {last_processed_height}")
        START_BLOCK = last_processed_height + 1 if last_processed_height is not None else 0
    else:
        START_BLOCK = 0

    END_BLOCK = end_block if end_block is not None else max_block_height_on_file

    # Fetch all non-coinbase transactions from the relevant blocks
    query_string = f"({START_BLOCK} <= index <= {END_BLOCK}) and (is_coinbase == False)"
    transactions_df_filtered = transactions_df.query(query_string)
    transactions_df_filtered = transactions_df_filtered.reset_index()
    transactions_to_fetch = transactions_df_filtered[['txid', 'height']].compute().values.tolist()
    print(f"Total transactions to fetch: {len(transactions_to_fetch)}")

    # Process the transactions in batches
    for i in range(0, len(transactions_to_fetch), BATCH_SIZE):
        batch_transactions = transactions_to_fetch[i:i + BATCH_SIZE]

        # VIN data extraction and batch save
        vin_df = fetch_vins_data(rpc_client, batch_transactions)
        if not vin_df.empty:
            save_batch(vin_df, input_directory, vins_schema)
            vin_batch_count += 1
        # print(f"Processed batch {vin_batch_count} (Transactions {i} to {i + BATCH_SIZE})")

        # Extract the range of height in the current batch
        heights_in_batch = [height for _, height in batch_transactions]
        min_height = min(heights_in_batch)
        max_height = max(heights_in_batch)
        print(f"Processed batch {vin_batch_count}, heights {min_height} to {max_height}")

    # Consolidate and clean up
    consolidate_parquet_files(input_directory, output_directory, write_index=True)
    delete_unconsolidated_directory(input_directory, output_directory)

def save_batch(data, directory, schema):
    """Save a batch of data to a parquet file."""
    df = pd.DataFrame(data)
    df['height'] = df['height'].astype('int32')
    df['vout'] = df['vout'].astype('int32')
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = ddf.set_index('height', sorted=True)
    ddf.to_parquet(directory, append=True, schema=schema, write_index=True, ignore_divisions=True)

def main():
    """Main function to run the vins population process."""
    parser = argparse.ArgumentParser(description="Process transactions from blocks.")
    parser.add_argument('--start', type=int, help='The starting block height', required=False)
    parser.add_argument('--end', type=int, help='The ending block height', required=False)
    
    args = parser.parse_args()

    env = setup_environment()
    vins_schema = define_schema()
    rpc_client = RPCClient()

    max_block_height_on_file = get_max_block_height_on_file(env=env)

    # Pass the start and end block arguments to process_vins
    process_vins(args.start, args.end, max_block_height_on_file, env, rpc_client, vins_schema)

if __name__ == "__main__":
    main()
