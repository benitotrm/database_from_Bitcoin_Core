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
