'''Module to populate the Transactions section of the database'''
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
        ('txid', pa.string()),
        ('block_hash', pa.string()),
        ('is_coinbase', pa.bool_())
    ])

def fetch_transaction_data(block_height, rpc_client, block_hashes_to_fetch):
    """Fetch data for a specific block."""
    try:
        responses = rpc_client.rpc_call_batch("getblock", [{"blockhash": h, "verbosity": 2} for h in block_hashes_to_fetch])
        
        if not responses:
            print(f"No response for block {block_height}")
            return []

        transactions = []
        for idx, response in enumerate(responses):
            # Ensure the response is valid
            if response is None:
                print(f"Response {idx} is None. Skipping.")
                continue
            
            if not isinstance(response, dict) or 'result' not in response:
                print(f"Invalid block response at index {idx}: {response}. Skipping.")
                continue
            
            block_info = response['result']

            # Process transactions within the block
            for tx in block_info.get('tx', []):
                transactions.append({
                    'txid': tx.get('txid'),
                    'block_hash': block_info.get('hash'),
                    'is_coinbase': 'coinbase' in tx.get('vin', [{}])[0]  # Handle missing 'vin'
                })
        return transactions

    except Exception as e:
        print(f"Error fetching transaction data for block {block_height}: {e}")
        return []

def process_transactions(start_block, end_block, max_block_height_on_file, env, rpc_client, transactions_schema):
    BLOCK_INCREASE = 10
    TRANSACTIONS_PER_BATCH = 10000
    BATCH_COUNT = 0

    input_directory = f"database/transaction_batches_{env}"
    output_directory = f"database/transactions_{env}"

    if not os.path.exists(input_directory):
        os.makedirs(input_directory)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    blocks_dir = os.path.join(os.path.dirname(__file__), f'../../database/blocks_{env}')
    blocks_df = dd.read_parquet(blocks_dir)

    # Debug: Check if blocks_df is loaded correctly
    print(f"Number of blocks available in blocks_df: {blocks_df.shape[0].compute()}")

    if start_block is not None:
        START_BLOCK = start_block
    elif os.path.exists(output_directory) and len(os.listdir(output_directory)) > 0:
        transactions_df = dd.read_parquet(output_directory)
        joined_df = transactions_df.merge(blocks_df[['block_hash', 'height']], on='block_hash', how='inner')
        last_processed_height = joined_df['height'].max().compute()
        START_BLOCK = last_processed_height + 1 if last_processed_height is not None else 0
    else:
        START_BLOCK = 0

    END_BLOCK = end_block if end_block is not None else max_block_height_on_file

    # Debug: Print START_BLOCK and END_BLOCK
    print(f"Starting block height: {START_BLOCK}")
    print(f"Ending block height: {END_BLOCK}")
    print(f"Maximum block height on file: {max_block_height_on_file}")

    while START_BLOCK <= END_BLOCK:
        current_end_block = min(START_BLOCK + BLOCK_INCREASE - 1, END_BLOCK)

        print(f"Processing blocks from {START_BLOCK} to {current_end_block}")

        # Filter blocks for the current range
        blocks_in_range_df = blocks_df[(blocks_df["height"] >= START_BLOCK) & (blocks_df["height"] <= current_end_block)].compute()
        block_hashes_to_fetch = blocks_in_range_df['block_hash'].tolist()

        # Fetch the transaction data for the blocks
        transactions_data = fetch_transaction_data(START_BLOCK, rpc_client, block_hashes_to_fetch)

        # 'transactions_data' contains the actual transactions, not block responses
        block_tx = []
        for idx, tx in enumerate(transactions_data):
            # Each item is a transaction, not a block response
            if not isinstance(tx, dict) or 'txid' not in tx:
                print(f"Invalid transaction at index {idx}: {tx}. Skipping.")
                continue

            block_tx.append(tx)  # Append the valid transaction to the list

            # If we've reached the transaction batch size, save the batch
            if len(block_tx) >= TRANSACTIONS_PER_BATCH:
                BATCH_COUNT += 1
                data = pd.DataFrame(block_tx[:TRANSACTIONS_PER_BATCH], columns=['txid', 'block_hash', 'is_coinbase'])
                save_batch(data, input_directory, transactions_schema)
                block_tx = block_tx[TRANSACTIONS_PER_BATCH:]  # Remove saved transactions

        # Save any remaining transactions in the final batch
        if block_tx:
            BATCH_COUNT += 1
            data = pd.DataFrame(block_tx, columns=['txid', 'block_hash', 'is_coinbase'])
            save_batch(data, input_directory, transactions_schema)

        # Increment START_BLOCK for the next iteration
        START_BLOCK = current_end_block + 1

    # Consolidate and clean up
    consolidate_parquet_files(input_directory, output_directory)
    delete_unconsolidated_directory(input_directory, output_directory)

def save_batch(data, directory, schema):
    """Save a batch of data to a parquet file."""
    ddf = dd.from_pandas(pd.DataFrame(data), npartitions=1)
    ddf.to_parquet(directory, append=True, schema=schema, write_index=False)

def main():
    """Main function to run the transaction population process."""
    # Argument parsing
    parser = argparse.ArgumentParser(description="Process transactions from blocks.")
    parser.add_argument('--start', type=int, help='The starting block height', required=False)
    parser.add_argument('--end', type=int, help='The ending block height', required=False)
    
    args = parser.parse_args()

    env = setup_environment()
    transactions_schema = define_schema()
    rpc_client = RPCClient()

    max_block_height_on_file = get_max_block_height_on_file(env=env)

    # Pass the start and end block arguments to process_transactions
    process_transactions(args.start, args.end, max_block_height_on_file, env, rpc_client, transactions_schema)

if __name__ == "__main__":
    main()
