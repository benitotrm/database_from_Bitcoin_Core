#%% ###################### Import libraries ######################
# Import libraries 
import glob
import os
import json
import requests
import shutil
import time
import dask.dataframe as dd
from dask import delayed
import pyarrow.parquet as pq
import pyarrow as pa

#%% ###################### Populate _transactions_parquets ###################### 
# ######################## Populate _transactions_parquets ################################
# Constants for batch processing
START_BLOCK = 667100
END_BLOCK = 667200
BLOCK_INCREASE = 100
FINAL_BLOCK = 768333
TRANSACTIONS_PER_BATCH = 100000
batch_count = 0

# Define the directory for the batch files
batch_directory = '_transactions_batches'
os.makedirs(batch_directory, exist_ok=True)

# Run the loop until we have processed all required blocks
while START_BLOCK < FINAL_BLOCK:
    blocks_df = dd.read_parquet('_blocks.parquet')
    filtered_blocks_df = blocks_df[(blocks_df["height"] >= START_BLOCK) & (blocks_df["height"] < END_BLOCK)].compute()

    transactions_directory = '_transactions.parquets'
    transactions_pattern = os.path.join(transactions_directory, '*.parquet')
    
    if os.path.exists(transactions_directory) and len(os.listdir(transactions_directory)) > 0:
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
                batch_count += 1
                batch_file_name = os.path.join(batch_directory, f'_transactions_batch_{batch_count}.parquet')
                batch_df.to_parquet(batch_file_name)
                block_tx = block_tx[TRANSACTIONS_PER_BATCH:]
                current_height = missing_blocks_df['height'].max() if 'height' in missing_blocks_df.columns else 'N/A'
                print(f"Batch {batch_count} processed successfully with block height up to {current_height}")
        else:
            print("Failed to process a batch of transactions")

    if block_tx:
        batch_count += 1
        batch_file_name = os.path.join(batch_directory, f'_transactions_batch_{batch_count}.parquet')
        pd.DataFrame(block_tx, columns=['txid', 'block_hash', 'is_coinbase']).to_parquet(batch_file_name)
        current_height = missing_blocks_df['height'].max() if 'height' in missing_blocks_df.columns else 'N/A'
        print(f"Final batch {batch_count} processed with block height up to {current_height}")

    START_BLOCK += BLOCK_INCREASE
    END_BLOCK = min(END_BLOCK + BLOCK_INCREASE, FINAL_BLOCK)

#%% ###################### Force consolidate ###################### 
# ################################## Force consolidate ################################
import pandas as pd
import math

input_directory = '__repartitioned'  # Directory with many small Parquet files
output_directory = '__consolidated'  # Directory for fewer, larger Parquet files
os.makedirs(output_directory, exist_ok=True)

# Define how many files to combine into one
files_per_combined_parquet = 500

# Get a list of all small Parquet files
all_files = [f for f in os.listdir(input_directory) if f.endswith('.parquet')]
total_files = len(all_files)
total_batches = math.ceil(total_files / files_per_combined_parquet)

# Process in batches
for i in range(total_batches):
    start_index = i * files_per_combined_parquet
    end_index = start_index + files_per_combined_parquet
    files_to_combine = all_files[start_index:end_index]

    # Read and concatenate the data from these files
    df_combined = pd.concat([pd.read_parquet(os.path.join(input_directory, f)) for f in files_to_combine], ignore_index=True)

    # Write the combined data to a new Parquet file
    output_file = os.path.join(output_directory, f'consolidated_{i+1}.parquet')
    df_combined.to_parquet(output_file, index=False)

print(f"Consolidation complete. Files written to {output_directory}")

#%% ######################  Partition size est. ######################  
################################### Partition size est. ############################### 
transactions_directory = '_transactions.parquets'
transactions_pattern = os.path.join(transactions_directory, '*.parquet')

# Get a list of all Parquet files in the directory
parquet_files = glob.glob(transactions_pattern)

# Ensure there is at least one file and read a sample from the first file
if parquet_files:
    sample_df = pq.read_table(parquet_files[0], columns=['txid', 'block_hash', 'is_coinbase']).to_pandas().head(1000)
    sample_df.to_parquet('sample.parquet')

    sample_file_size = os.path.getsize('sample.parquet') / 1024  # Size in KB
    average_row_size_kb = sample_file_size / 1000
    print(f"Average row size: {average_row_size_kb} KB")
else:
    print("No Parquet files found in the directory.")

#%% ######################  Force repartition ############################### 
################################### Force repartition ############################### 
# New directory containing all the Parquet files
combined_directory = '__consolidated'
output_directory = '__repartitioned'

# Ensure output directory exists
os.makedirs(output_directory, exist_ok=True)

# Define the chunk size for repartitioning
chunk_size = 18000000  # Adjust as needed

# Process and write out each file in the combined directory
for file in os.listdir(combined_directory):
    file_path = os.path.join(combined_directory, file)
    parquet_file = pq.ParquetFile(file_path)

    for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
        table = pa.Table.from_batches([batch])
        output_file = os.path.join(output_directory, f'{file}_chunk_{i}.parquet')
        pq.write_table(table, output_file)

# %% ############################### RAW BLOCKS ################################
# ################################## RAW BLOCKS ################################
# Constants for batch processing
BATCH_SIZE = 10 
BLOCK_DIRECTORY = '_block_batches'
os.makedirs(BLOCK_DIRECTORY, exist_ok=True)

# Function to retrieve block data in batches using rpc_call_batch
def fetch_block_data(block_hashes):
    responses = rpc_call_batch("getblock", [{"blockhash": h, "verbosity": 2} for h in block_hashes])
    return responses

# Load block hashes from _blocks.parquet
blocks_df = pl.read_parquet('_blocks.parquet')
block_hashes = blocks_df['block_hash'].to_list()

# Process blocks in batches
for i in range(0, len(block_hashes), BATCH_SIZE):
    batch_hashes = block_hashes[i:i+BATCH_SIZE]
    block_data = fetch_block_data(batch_hashes)

    # Process and save each block's data
    for j, response in enumerate(block_data):
        if response is not None:
            block_raw_data = response['result']
            # Convert raw data to DataFrame - adjust as needed for your data structure
            df = pl.DataFrame([block_raw_data])
            batch_file_name = os.path.join(BLOCK_DIRECTORY, f'block_data_batch_{i+j}.parquet')
            df.write_parquet(batch_file_name)
        else:
            print(f"Failed to fetch data for block hash {batch_hashes[j]}")

# Consolidate batch files into a single DataFrame
pattern = os.path.join(BLOCK_DIRECTORY, 'block_data_batch_*.parquet')
batch_files = glob.glob(pattern)
combined_blocks_df = pl.concat([pl.read_parquet(f) for f in batch_files])

# Write the combined DataFrame to a single Parquet file
combined_blocks_df.write_parquet('combined_block_data.parquet')

# Optional: Clean up the batch files and remove the temporary folder
shutil.rmtree(BLOCK_DIRECTORY)

# %% ######################  RAW TRANSACTIONS ################################
# ################################## RAW TRANSACTIONS ################################

# Directory to save raw transactions
raw_transactions_directory = '_raw_transactions_parquets'
os.makedirs(raw_transactions_directory, exist_ok=True)

# Constants for the period
START_DATE = pd.to_datetime("2017-11-01")
END_DATE = pd.to_datetime("2017-11-30")

# Read blocks data
blocks_df = dd.read_parquet('_blocks.parquet')

# Convert 'time' column from Unix epoch time to datetime
blocks_df['time'] = dd.to_datetime(blocks_df['time'], unit='s')

# Filter blocks within the specified period and persist
filtered_blocks_df = blocks_df[(blocks_df['time'] >= START_DATE) & (blocks_df['time'] <= END_DATE)].persist()

# Read all transactions parquets
transactions_directory = '_transactions_parquets'
transactions_pattern = os.path.join(transactions_directory, '*.parquet')
transactions_df = dd.read_parquet(transactions_pattern)

# Join with blocks to get transactions in the specified period and persist
joined_df = transactions_df.merge(filtered_blocks_df, on='block_hash', how='inner').persist()

# Get unique transaction IDs along with block height and time, then compute
transaction_details = joined_df[['txid', 'height', 'time']].compute()

# Creating a dictionary for quick lookups
transaction_details_dict = transaction_details.set_index('txid').to_dict(orient='index')

# Function to batch retrieve raw transaction details
def get_raw_transactions(txids):
    transactions_data = rpc_call_batch("getrawtransaction", [{"txid": txid, "verbose": True} for txid in txids])
    raw_transactions = []
    for response in transactions_data:
        if response and 'result' in response:
            raw_transactions.append(response['result'])
    return raw_transactions

# Process in batches to avoid memory issues
BATCH_SIZE = 10000  # Adjust as needed
batch_count = 0

# Process transactions in batches
for i in range(0, len(transaction_details), BATCH_SIZE):
    batch_txids = transaction_details['txid'][i:i + BATCH_SIZE]
    raw_transactions = get_raw_transactions(batch_txids)

    if raw_transactions:
        # Combine with block height and time using the dictionary
        for tx in raw_transactions:
            tx_details = transaction_details_dict.get(tx['txid'], {})
            tx['height'] = tx_details.get('height')
            tx['time'] = tx_details.get('time')

        # Convert to DataFrame and save as Parquet
        batch_df = pd.DataFrame(raw_transactions)
        batch_file_name = os.path.join(raw_transactions_directory, f'raw_transactions_batch_{batch_count}.parquet')
        batch_df.to_parquet(batch_file_name)
        batch_count += 1

    print(f"Processed batch {i // BATCH_SIZE + 1}/{len(transaction_details) // BATCH_SIZE + 1}")

# Inform about completion
print(f"Processed {batch_count} batches in total.")
# %% ######################  RENAME FILES ################################
# ################################## RENAME FILES ################################

# Directory containing the .parquet files
directory = '_vout_parquets'

# Get all .parquet files and their sizes
files = [(f, os.path.getsize(os.path.join(directory, f))) for f in os.listdir(directory) if f.endswith('.parquet')]

# Sort files by size in descending order
files.sort(key=lambda x: x[1], reverse=True)

# First, rename all files to a temporary name
temp_names = {}
for i, (filename, _) in enumerate(files, start=1):
    temp_name = f"temp_{i}.parquet"
    os.rename(os.path.join(directory, filename), os.path.join(directory, temp_name))
    temp_names[temp_name] = i

# Then, rename them to their final names
for temp_name, i in temp_names.items():
    final_name = f"{i}.parquet"
    os.rename(os.path.join(directory, temp_name), os.path.join(directory, final_name))

print("Files have been renamed successfully.")

# %% ######################  Populate _vin_parquets ###################### 
# ################################## Populate _vin_parquets ################################
# Constants for batch processing
START_BLOCK = 700000
FINAL_BLOCK = 768333
BATCH_SIZE = 10000  
vin_batch_count = 0

# Define the directory for the VIN batch files
vin_batch_directory = '_vin_batches'
os.makedirs(vin_batch_directory, exist_ok=True)

@delayed
def get_vin_data(transactions_to_fetch):
    vin_data = rpc_call_batch("getrawtransaction", [{"txid": txid, "verbose": 1} for txid in transactions_to_fetch])

    vin_rows = []
    for response in vin_data:
        if response is not None:
            txid = response['result']['txid']
            for vin in response['result']['vin']:
                if 'txid' in vin:
                    vin_txid = vin['txid']
                    vout = vin['vout']
                    vin_rows.append((txid, vin_txid, vout))
    return pd.DataFrame(vin_rows, columns=['txid', 'vin_txid', 'vout'])

# Read the blocks and transactions parquets
blocks_df = dd.read_parquet('_blocks.parquet')
transactions_df = dd.read_parquet(os.path.join('_transactions_parquets', '*.parquet'))

# Filter blocks and get the corresponding block hashes
filtered_blocks_df = blocks_df[(blocks_df["height"] >= START_BLOCK) & (blocks_df["height"] < FINAL_BLOCK)]
block_hashes = filtered_blocks_df['block_hash'].compute().tolist()
print("Blocks filtered.")

# Select transactions in the filtered blocks and not coinbase
transactions_to_fetch = transactions_df[transactions_df['block_hash'].isin(block_hashes) & (transactions_df['is_coinbase'] == False)]['txid'].compute().tolist()
print("Transactions selected.")

# Process the transactions in batches
for i in range(0, len(transactions_to_fetch), BATCH_SIZE):
    batch_transactions = transactions_to_fetch[i:i + BATCH_SIZE]

    # Delayed VIN data extraction
    vin_df = get_vin_data(batch_transactions)

    # Perform final compute here
    vin_df_computed = vin_df.compute()
    if not vin_df_computed.empty:
        vin_batch_file_name = os.path.join(vin_batch_directory, f'_vin_data_batch_{vin_batch_count}.parquet')
        vin_df_computed.to_parquet(vin_batch_file_name)
        vin_batch_count += 1
    print(f"Processed batch {vin_batch_count} (Transactions {i} to {i + BATCH_SIZE})")

#%% ######################  Populate _vout_parquets ###################### 
# ######################  Populate _vout_parquets ###################### 
# Constants for batch processing
START_BLOCK = 700000
FINAL_BLOCK = 768333
BATCH_SIZE = 10000

# Define the directory for the VOUT batch files
vout_batch_directory = '_vout_batches'
os.makedirs(vout_batch_directory, exist_ok=True)

@delayed
def get_vout_data(transactions_to_fetch):
    vout_data = rpc_call_batch("getrawtransaction", [{"txid": txid, "verbose": 1} for txid in transactions_to_fetch])

    vout_rows = []
    for response in vout_data:
        if response is not None:
            txid = response['result']['txid']
            for vout in response['result']['vout']:
                value = vout['value']
                n = vout['n']
                scriptPubKey = vout.get('scriptPubKey', {})
                addresses = scriptPubKey.get('addresses', None)
                if addresses is None:
                    address = scriptPubKey.get('address', None)
                    addresses = [address] if address else []
                
                # Only add rows where addresses are explicit
                if addresses:
                    vout_rows.append((txid, value, n, addresses))
    return pd.DataFrame(vout_rows, columns=['txid', 'value', 'n', 'addresses'])

# Read the blocks and transactions parquets
blocks_df = dd.read_parquet('_blocks.parquet')
transactions_df = dd.read_parquet(os.path.join('_transactions_parquets', '*.parquet'))

# Filter blocks and get the corresponding block hashes
filtered_blocks_df = blocks_df[(blocks_df["height"] >= START_BLOCK) & (blocks_df["height"] < FINAL_BLOCK)]
block_hashes = filtered_blocks_df['block_hash'].compute().tolist()

# Select transactions in the filtered blocks
transactions_to_fetch = transactions_df[transactions_df['block_hash'].isin(block_hashes)]['txid'].compute().tolist()

# Process the transactions in batches
vout_batch_count = 0
for i in range(0, len(transactions_to_fetch), BATCH_SIZE):
    batch_transactions = transactions_to_fetch[i:i + BATCH_SIZE]

    # Delayed VOUT data extraction
    vout_df = get_vout_data(batch_transactions)

    # Perform final compute here
    vout_df_computed = vout_df.compute()
    if not vout_df_computed.empty:
        vout_batch_file_name = os.path.join(vout_batch_directory, f'_vout_data_batch_{vout_batch_count}.parquet')
        vout_df_computed.to_parquet(vout_batch_file_name)
        vout_batch_count += 1
    print(f"Processed batch {vout_batch_count} (Transactions {i} to {i + BATCH_SIZE})")


# %%
