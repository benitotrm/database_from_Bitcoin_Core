"""Module to populate the Blocks section of the database"""
import os
import json
import argparse
import requests
import pandas as pd
import pyarrow as pa
import dask.dataframe as dd
from src.api.rpc_client import RPCClient
from src.utils.commons import (get_current_branch, get_max_block_height_on_file, 
                               consolidate_parquet_files, delete_unconsolidated_directory)

def setup_environment():
    """Set up the environment and print initial information."""
    print("\npopulate_blocks.py started...\n")
    branch_name = get_current_branch()
    print(f"Current branch: {branch_name}")
    return 'main' if branch_name == 'main' else 'dev'

def define_schema():
    """Define and return the schema for blocks data."""
    return pa.schema([
        ('block_hash', pa.string()),
        ('height', pa.int64()),
        ('time', pa.int64()),
        ('tx_count', pa.int64())
    ])

def fetch_block_data(block_height, rpc_client):
    """Fetch data for a specific block."""
    try:
        response = rpc_client.rpc_call_batch("getblockstats", [[block_height]])[0]
        if not response or ('error' in response and response['error'] is not None):
            print(f"Failed to fetch block stats for block {block_height}.")
            return None

        block_stats = response['result']
        return {
            'block_hash': block_stats['blockhash'],
            'height': block_height,
            'time': block_stats['time'],
            'tx_count': block_stats['txs']
        }
    except requests.exceptions.RequestException as req_err:
        print(f"Network error at block {block_height}: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"JSON decoding error at block {block_height}: {json_err}")
    except KeyError as key_err:
        print(f"Missing expected data in block {block_height}: {key_err}")
    return None

def get_latest_block_height(rpc_client):
    """Get the latest block height from the node."""
    response = rpc_client.rpc_call_batch("getblockcount", [[]])[0]
    if 'result' in response:
        return response['result']
    else:
        raise ValueError("Failed to fetch the latest block height from the node.")

def process_blocks(start, end, env, rpc_client, blocks_schema):
    """Process and save block data in batches."""
    BATCH_SIZE = 1000
    input_directory = f"database/blocks_batches_{env}"
    output_directory = f"database/blocks_{env}"
    
    if not os.path.exists(input_directory):
        os.makedirs(input_directory)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    max_block_height_on_file = get_max_block_height_on_file(env=env)
    data = []

    for block_height in range(start, end + 1):
        if max_block_height_on_file and block_height <= max_block_height_on_file:
            print(f"Block {block_height} already exists. Skipping.")
            continue

        row = fetch_block_data(block_height, rpc_client)
        if row:
            data.append(row)

        if len(data) >= BATCH_SIZE:
            save_batch(data, input_directory, blocks_schema)
            data = []
            print(f"Saved up to block {block_height}")

    if data:
        save_batch(data, input_directory, blocks_schema)

    consolidate_parquet_files(input_directory, output_directory)
    delete_unconsolidated_directory(input_directory, output_directory)

def save_batch(data, directory, schema):
    """Save a batch of data to a parquet file."""
    ddf = dd.from_pandas(pd.DataFrame(data), npartitions=1)
    ddf.to_parquet(directory, append=True, schema=schema, write_index=False)

def main():
    """Main function to run the block population process."""
    env = setup_environment()
    blocks_schema = define_schema()
    rpc_client = RPCClient()
    
    parser = argparse.ArgumentParser(description="Populate blocks folder.")
    parser.add_argument("--start", type=int, help="Starting block height.")
    parser.add_argument("--end", type=int, help="Ending block height.")
    args = parser.parse_args()

    start = args.start if args.start is not None else (get_max_block_height_on_file(env=env) + 1 if get_max_block_height_on_file(env=env) is not None else 0)
    end = args.end if args.end is not None else get_latest_block_height(rpc_client)

    print(f"Starting block: {start}, Ending block: {end}")
    process_blocks(start, end, env, rpc_client, blocks_schema)
    print("Populating blocks table completed.")

if __name__ == "__main__":
    main()