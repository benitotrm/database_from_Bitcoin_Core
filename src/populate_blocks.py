"""Module to populate the Blocks section of the database"""
import os
import json
import argparse
import requests
import pandas as pd
import dask.dataframe as dd
import pyarrow as pa
from src.api.rpc_client import RPCClient

# Define the schema using pyarrow
blocks_schema = pa.schema([
    ('block_hash', pa.string()),
    ('height', pa.int64()),
    ('time', pa.int64()),
    ('tx_count', pa.int64())
])

def fetch_block_data(block_height, rpc_client):
    """Calls the RPCCLient to fetch the Block Data"""
    try:
        response = rpc_client.rpc_call_batch("getblockstats", [[block_height]])[0]

        if not response or 'error' in response and response['error'] is not None:
            print(f"Failed to fetch block stats for block {block_height}.")
            return None

        block_stats = response['result']

        block_id = block_stats['blockhash']
        timestamp = block_stats['time']
        tx_count = block_stats['txs']

        return {
            'block_hash': block_id,
            'height': block_height,
            'time': timestamp,
            'tx_count': tx_count
        }
    except requests.exceptions.RequestException as req_err:
        print(f"Network error at block {block_height}: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"JSON decoding error at block {block_height}: {json_err}")
    except KeyError as key_err:
        print(f"Missing expected data in block {block_height}: {key_err}")
    return None

def get_max_block_height_on_file(consolidated_output_directory):
    """Returns the maximum block height available in the existing Parquet files"""
    if os.path.exists(consolidated_output_directory):
        parquet_files = [os.path.join(consolidated_output_directory, f) for f in os.listdir(consolidated_output_directory) if f.endswith(".parquet")]
        if parquet_files:
            existing_blocks_df = dd.read_parquet(parquet_files)
            if existing_blocks_df.shape[0].compute() > 0:
                return existing_blocks_df['height'].max().compute()
    return None

def get_latest_block_height_from_node(rpc_client):
    """Returns the latest block height from the Bitcoin node"""
    response = rpc_client.rpc_call_batch("getblockcount", [[]])[0]
    if 'result' in response:
        return response['result']
    else:
        raise ValueError("Failed to fetch the latest block height from the node.")

def populate_blocks(start=None, end=None):
    """Populates the blocks.parquets folder with Block information"""
    rpc_client = RPCClient()
    batch_size = 1000
    output_directory = "database/blocks.parquets"
    consolidated_output_directory = "database/consolidated_blocks.parquets"

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    if not os.path.exists(consolidated_output_directory):
        os.makedirs(consolidated_output_directory)

    max_block_height_on_file = get_max_block_height_on_file(consolidated_output_directory)
    latest_block_height_from_node = get_latest_block_height_from_node(rpc_client)

    # Set start and end defaults if not provided
    if start is None:
        start = max_block_height_on_file + 1 if max_block_height_on_file is not None else 0
    if end is None:
        end = latest_block_height_from_node

    print(f"Starting block: {start}, Ending block: {end}")

    data = []

    for block_height in range(start, end + 1):
        if max_block_height_on_file and block_height <= max_block_height_on_file:
            print(f"Block {block_height} already exists. Skipping.")
            continue

        row = fetch_block_data(block_height, rpc_client)
        if row:
            data.append(row)

        if len(data) >= batch_size:
            ddf = dd.from_pandas(pd.DataFrame(data), npartitions=1)
            ddf.to_parquet(output_directory, append=True, write_index=False, schema=blocks_schema)
            data = []
            print(f"Saved up to block {block_height}")

    # Write remaining data
    if data:
        ddf = dd.from_pandas(pd.DataFrame(data), npartitions=1)
        ddf.to_parquet(output_directory, append=True, write_index=False, schema=blocks_schema)

    # Consolidate small Parquet files into larger files
    consolidate_parquet_files(output_directory, consolidated_output_directory)

    print("Populating blocks table completed.")

def consolidate_parquet_files(input_directory, output_directory):
    """Consolidates small Parquet files into larger files of approximately 1GB"""

    df = dd.read_parquet(f'{input_directory}/*.parquet')

    target_partition_size = 1e9  # 1GB in bytes
    current_size = df.memory_usage(deep=True).sum().compute()
    npartitions = max(1, int(current_size / target_partition_size))

    df = df.repartition(npartitions=npartitions)
    df.to_parquet(output_directory, write_metadata_file=True)

    print(f"Consolidated Parquet files written to {output_directory}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate the blocks.parquets folder with block information.")
    parser.add_argument("--start", type=int, help="Starting block height")
    parser.add_argument("--end", type=int, help="Ending block height")
    
    args = parser.parse_args()
    populate_blocks(start=args.start, end=args.end)

