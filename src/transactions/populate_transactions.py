'''Module to populate the Transactions section of the database'''
import os
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
        for response in responses:
            if response.get('error'):
                print(f"Error in response for block hash {response.get('blockhash')}: {response['error']}")
                continue
            block_info = response.get('result', {})
            for tx in block_info.get('tx', []):
                transactions.append({
                    'txid': tx.get('txid'),
                    'block_hash': block_info.get('hash'),
                    'is_coinbase': 'coinbase' in tx.get('vin', [{}])[0]
                })
        return transactions
    except Exception as e:
        print(f"Error fetching transaction data for block {block_height}: {e}")
        return []

def process_transactions(max_block_height_on_file, env, rpc_client, transactions_schema):
    BLOCK_INCREASE = 100
    TRANSACTIONS_PER_BATCH = 100000
    BATCH_COUNT = 0

    input_directory = f"database/transaction_batches_{env}"
    output_directory = f"database/transactions_{env}"

    if not os.path.exists(input_directory):
        os.makedirs(input_directory)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    blocks_dir = os.path.join(os.path.dirname(__file__), f'../../database/blocks_{env}')
    blocks_df = dd.read_parquet(blocks_dir)
  
    if os.path.exists(output_directory) and len(os.listdir(output_directory)) > 0:
        transactions_df = dd.read_parquet(output_directory)
        last_processed_height = transactions_df['height'].max().compute()
        START_BLOCK = last_processed_height + 1 if last_processed_height is not None else 0
    else:
        START_BLOCK = 0

    END_BLOCK = max_block_height_on_file

    missing_blocks_df = blocks_df[(blocks_df["height"] >= START_BLOCK) & (blocks_df["height"] < END_BLOCK)].compute()

    # Run the loop until we have processed all required blocks
    while START_BLOCK < END_BLOCK:

        block_hashes_to_fetch = missing_blocks_df['block_hash'].to_list()
        transactions_data = fetch_transaction_data(START_BLOCK, rpc_client, block_hashes_to_fetch)

        block_tx = []
        for response in transactions_data:
            if response is not None:
                for tx in response['result']['tx']:
                    is_coinbase_tx = 'coinbase' in tx['vin'][0] if tx['vin'] else False
                    block_tx.append((tx['txid'], response['result']['hash'], is_coinbase_tx))

                if len(block_tx) >= TRANSACTIONS_PER_BATCH:
                    BATCH_COUNT += 1
                    data = pd.DataFrame(block_tx[:TRANSACTIONS_PER_BATCH], columns=['txid', 'block_hash', 'is_coinbase'])
                    save_batch(data, input_directory, transactions_schema)

                    block_tx = block_tx[TRANSACTIONS_PER_BATCH:]
                    current_height = missing_blocks_df['height'].max()
                    print(f"Batch {BATCH_COUNT} processed successfully with block height up to {current_height}")
            else:
                print("Failed to process a batch of transactions")

        if block_tx:
            BATCH_COUNT += 1
            data = pd.DataFrame(block_tx, columns=['txid', 'block_hash', 'is_coinbase'])
            save_batch(data, input_directory, transactions_schema)
            current_height = missing_blocks_df['height'].max()
            print(f"Final batch {BATCH_COUNT} processed with block height up to {current_height}")

        START_BLOCK = current_height + 1 if current_height is not None else 0
        END_BLOCK = min(START_BLOCK + BLOCK_INCREASE - 1, max_block_height_on_file)

    consolidate_parquet_files(input_directory, output_directory)
    delete_unconsolidated_directory(input_directory, output_directory)

def save_batch(data, directory, schema):
    """Save a batch of data to a parquet file."""
    ddf = dd.from_pandas(pd.DataFrame(data), npartitions=1)
    ddf.to_parquet(directory, append=True, schema=schema, write_index=False)

def main():
    """Main function to run the transaction population process."""
    env = setup_environment()
    transactions_schema = define_schema()
    rpc_client = RPCClient()

    max_block_height_on_file = get_max_block_height_on_file(env=env)    
    process_transactions(max_block_height_on_file, env, rpc_client, transactions_schema)

if __name__ == "__main__":
    main()
