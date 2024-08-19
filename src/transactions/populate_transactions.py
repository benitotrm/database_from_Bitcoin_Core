'''Module to populate the Transactions section of the database'''
import os
import dask.dataframe as dd
from src.api.rpc_client import RPCClient

# Constants for batch processing
START_BLOCK = 667100
END_BLOCK = 667200
BLOCK_INCREASE = 100
FINAL_BLOCK = 768333
TRANSACTIONS_PER_BATCH = 100000
BATCH_COUNT = 0

# Define the directory for the batch files
BATCH_DIRECTORY = 'transactions_batches'
os.makedirs(BATCH_DIRECTORY, exist_ok=True)

# Run the loop until we have processed all required blocks
while START_BLOCK < FINAL_BLOCK:
    blocks_df = dd.read_parquet('_blocks.parquet')
    filtered_blocks_df = blocks_df[(blocks_df["height"] >= START_BLOCK) & (blocks_df["height"] < END_BLOCK)].compute()

    TRANSACTIONS_DIRECTORY = '_transactions.parquets'
    transactions_pattern = os.path.join(TRANSACTIONS_DIRECTORY, '*.parquet')
    
    if os.path.exists(TRANSACTIONS_DIRECTORY) and len(os.listdir(TRANSACTIONS_DIRECTORY)) > 0:
        transactions_df = dd.read_parquet(transactions_pattern)
        missing_blocks_df = filtered_blocks_df[~filtered_blocks_df['block_hash'].isin(transactions_df['block_hash'].unique().compute())]
    else:
        missing_blocks_df = filtered_blocks_df

    block_hashes_to_fetch = missing_blocks_df['block_hash'].to_list()
    transactions_data = rpc_call_batch("getblock", [{"blockhash": h, "verbosity": 2} for h in block_hashes_to_fetch])

    block_tx = []
    for response in transactions_data:
        if response is not None:
            for tx in response['result']['tx']:
                is_coinbase_tx = 'coinbase' in tx['vin'][0] if tx['vin'] else False
                block_tx.append((tx['txid'], response['result']['hash'], is_coinbase_tx))

            if len(block_tx) >= TRANSACTIONS_PER_BATCH:
                batch_df = pd.DataFrame(block_tx[:TRANSACTIONS_PER_BATCH], columns=['txid', 'block_hash', 'is_coinbase'])
                BATCH_COUNT += 1
                batch_file_name = os.path.join(BATCH_DIRECTORY, f'_transactions_batch_{BATCH_COUNT}.parquet')
                batch_df.to_parquet(batch_file_name)
                block_tx = block_tx[TRANSACTIONS_PER_BATCH:]
                current_height = missing_blocks_df['height'].max() if 'height' in missing_blocks_df.columns else 'N/A'
                print(f"Batch {BATCH_COUNT} processed successfully with block height up to {current_height}")
        else:
            print("Failed to process a batch of transactions")

    if block_tx:
        BATCH_COUNT += 1
        batch_file_name = os.path.join(BATCH_DIRECTORY, f'_transactions_batch_{BATCH_COUNT}.parquet')
        pd.DataFrame(block_tx, columns=['txid', 'block_hash', 'is_coinbase']).to_parquet(batch_file_name)
        current_height = missing_blocks_df['height'].max() if 'height' in missing_blocks_df.columns else 'N/A'
        print(f"Final batch {BATCH_COUNT} processed with block height up to {current_height}")

    START_BLOCK += BLOCK_INCREASE
    END_BLOCK = min(END_BLOCK + BLOCK_INCREASE, FINAL_BLOCK)