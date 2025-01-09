import os
import argparse
import polars as pl
from src.api.rpc_client import RPCClient
from src.utils.commons import (get_current_branch, get_max_block_height_on_file, 
                               consolidate_parquet_files, delete_unconsolidated_directory)
import hashlib
import base58

def setup_environment():
    """Set up the environment and print initial information."""
    print("\nRunning populate_vouts.py\n")
    branch_name = get_current_branch()
    print(f"Current branch: {branch_name}")
    return 'main' if branch_name == 'main' else 'dev'

def base58check_encode(version, payload):
    """Encode a payload with a version byte using Base58Check."""
    versioned_payload = bytes([version]) + payload
    checksum = hashlib.sha256(hashlib.sha256(versioned_payload).digest()).digest()[:4]
    return base58.b58encode(versioned_payload + checksum).decode('utf-8')

def derive_address(script_pub_key):
    """Derive Bitcoin address from scriptPubKey."""
    script_type = script_pub_key.get('type')
    asm = script_pub_key.get('asm', "")

    if 'address' in script_pub_key:
        return script_pub_key['address']

    try:
        if script_type == "pubkey":
            pubkey = bytes.fromhex(asm.split()[0])
            hash160 = hashlib.new('ripemd160', hashlib.sha256(pubkey).digest()).digest()
            return base58check_encode(0x00, hash160)  # P2PK mainnet

        elif script_type == "pubkeyhash":
            hash160 = bytes.fromhex(asm.split()[1])
            return base58check_encode(0x00, hash160)  # P2PKH mainnet

        elif script_type == "scripthash":
            hash160 = bytes.fromhex(asm.split()[1])
            return base58check_encode(0x05, hash160)  # P2SH mainnet

    except Exception as e:
        print(f"Error deriving address for script: {script_pub_key}. Error: {e}")

    return None  # Return None for unsupported or non-standard scripts

def fetch_vouts_data(rpc_client, transactions_with_height):
    """Fetch data for a specific transaction along with height."""
    vout_data = rpc_client.rpc_call_batch("getrawtransaction", [{"txid": str(txid), "verbose": 1} for txid, height in transactions_with_height])

    vout_rows = []
    for (txid, height), response in zip(transactions_with_height, vout_data):
        if response is not None:
            txid = response['result']['txid']
            for vout in response['result']['vout']:
                value = vout['value']
                n = vout['n']
                script_pub_key = vout.get('scriptPubKey', {})

                # Check for 'addresses' field and concatenate all addresses if they exist
                address = None
                if 'addresses' in script_pub_key:
                    address = ",".join(script_pub_key['addresses'])  # Concatenate all addresses with commas
                else:
                    address = derive_address(script_pub_key)

                script_type = script_pub_key.get('type', None)
                vout_rows.append((height, txid, value, n, address, script_type))
        else:
            print(f"No response for transaction: {txid}")

    return pl.DataFrame(vout_rows, schema=[("height", pl.Int32), ("txid", pl.Utf8), ("value", pl.Float64), ("n", pl.Int32), ("address", pl.Utf8), ("script_type", pl.Utf8)], orient="row")

def process_vouts(start_block, end_block, max_block_height_on_file, env, rpc_client):
    # Constants for batch processing
    BATCH_SIZE = 100
    vout_batch_count = 0

    input_directory = f"database/vouts_batches_{env}"
    output_directory = f"database/vouts_{env}"

    if not os.path.exists(input_directory):
        os.makedirs(input_directory)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    transactions_dir = os.path.join(os.path.dirname(__file__), f'../../database/transactions_{env}')

    # Use scan_parquet for efficient lazy loading
    transactions_df = pl.scan_parquet(transactions_dir).select(["txid", "is_coinbase", "height"])

    if start_block is not None:
        START_BLOCK = start_block
    elif os.path.exists(output_directory) and len(os.listdir(output_directory)) > 0:
        vouts_df = pl.read_parquet(output_directory)
        last_processed_height = vouts_df.select(pl.col("height").max()).to_numpy()[0][0]
        print(f"Last processed height: {last_processed_height}")
        START_BLOCK = last_processed_height + 1 if last_processed_height is not None else 0
    else:
        START_BLOCK = 0

    END_BLOCK = end_block if end_block is not None else max_block_height_on_file

    print(f"Processing blocks from {START_BLOCK} to {END_BLOCK}")

    # Fetch all non-coinbase transactions from the relevant blocks
    transactions_df_filtered = transactions_df.filter(
        (pl.col("height") >= START_BLOCK) & (pl.col("height") <= END_BLOCK) & (~pl.col("is_coinbase"))
    ).sort("height").collect()

    transactions_to_fetch = transactions_df_filtered.select(["txid", "height"]).to_numpy().tolist()
    print(f"Total transactions to fetch: {len(transactions_to_fetch)}")

    # Process the transactions in batches
    for i in range(0, len(transactions_to_fetch), BATCH_SIZE):
        batch_transactions = transactions_to_fetch[i:i + BATCH_SIZE]

        # VOUT data extraction and batch save
        vout_df = fetch_vouts_data(rpc_client, batch_transactions)
        if not vout_df.is_empty():
            save_batch(vout_df, input_directory, vout_batch_count)
            vout_batch_count += 1
        else:
            print(f"Batch {vout_batch_count + 1} returned no data.")

        # Extract the range of height in the current batch
        heights_in_batch = [height for _, height in batch_transactions]
        min_height = min(heights_in_batch)
        max_height = max(heights_in_batch)

        # Print status every x batches
        if vout_batch_count % 1000 == 0:
            print(f"Processed batch {vout_batch_count}, up to height {max_height}")

    # Consolidate and clean up
    print(f"Consolidating batches from {input_directory} into {output_directory}")
    consolidate_parquet_files(input_directory, output_directory, target_partition_size='2GB', write_index=True)
    delete_unconsolidated_directory(input_directory, output_directory)
    print("Processing complete.")

def save_batch(data, directory, batch_number):
    """Save a batch of data to a parquet file."""
    file_path = os.path.join(directory, f"batch_{batch_number}.parquet")
    data.write_parquet(file_path, compression=None)

def main():
    """Main function to run the vouts population process."""
    parser = argparse.ArgumentParser(description="Process transactions from blocks.")
    parser.add_argument('--start', type=int, help='The starting block height', required=False)
    parser.add_argument('--end', type=int, help='The ending block height', required=False)
    
    args = parser.parse_args()

    env = setup_environment()
    rpc_client = RPCClient()

    max_block_height_on_file = get_max_block_height_on_file(env=env)

    # Pass the start and end block arguments to process_vouts
    process_vouts(args.start, args.end, max_block_height_on_file, env, rpc_client)

if __name__ == "__main__":
    main()
