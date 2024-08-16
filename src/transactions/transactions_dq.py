# Load the transactions and blocks data
transactions_directory = '_transactions_parquets'

transactions_df = os.path.join(transactions_directory, '*.parquet')
df = dd.read_parquet(transactions_df)
blocks_df = dd.read_parquet('_blocks.parquet')

# Persist the DataFrames in memory for faster access
df = df.persist()
blocks_df = blocks_df.persist()

# Calculate statistics
unique_hashes = df['block_hash'].nunique()
unique_tx = df['txid'].nunique()
null_txid_count = df['txid'].isnull().sum()
null_block_hash_count = df['block_hash'].isnull().sum()

# Perform all computations in a single step
unique_hashes, unique_tx, null_txid_count, null_block_hash_count = dd.compute(
    unique_hashes, unique_tx, null_txid_count, null_block_hash_count)

# Calculate the number of coinbase transactions
coinbase_tx_count = df[df['is_coinbase']].shape[0].compute()

# Print statistics including the coinbase transaction check
print(f"\nTotal Number of Blocks: {unique_hashes}")
print(f"Total Number of Transactions: {len(df)}")
print(f"Number of Unique Transactions: {unique_tx}")
print(f"Number of Transactions with Missing txid: {null_txid_count}")
print(f"Number of Transactions with Missing block_hash: {null_block_hash_count}")
print(f"Number of Coinbase Transactions: {coinbase_tx_count}")

# Check if coinbase transactions match the number of blocks
if coinbase_tx_count != unique_hashes:
    print(f"Discrepancy Alert: Number of coinbase transactions ({coinbase_tx_count}) does not match the number of blocks ({unique_hashes}).")
else:
    print("Coinbase transaction count matches the number of blocks.")

# Check for duplicates
duplicates_count = df.group_by('txid').size()
duplicates = duplicates_count[duplicates_count > 1].compute()
if len(duplicates) > 0:
    print("\nSample of Duplicate Transactions:")
    print(duplicates.head())

# Join transactions with blocks to test for block height continuity
joined_df = df.merge(blocks_df, left_on='block_hash', right_on='block_hash', how='inner')

# Compute min and max block heights from joined DataFrame
min_block_height = joined_df['height'].min().compute()
max_block_height = joined_df['height'].max().compute()
total_expected_blocks = max_block_height - min_block_height + 1

# Continuity check
if total_expected_blocks != unique_hashes:
    print(f"\nBlock Heights are not continuous. Expected {total_expected_blocks} blocks, found {unique_hashes}.")

    # Find block height with missing coinbase transaction
    coinbase_tx_per_block = joined_df[joined_df['is_coinbase']].group_by('height').size()
    all_block_heights = coinbase_tx_per_block.index.compute()
    missing_coinbase_blocks = set(range(min_block_height, max_block_height + 1)) - set(all_block_heights)
    
    if missing_coinbase_blocks:
        print("Missing Coinbase Transactions at Block Heights:")
        print(missing_coinbase_blocks)
    else:
        print("No missing Coinbase Transactions found.")
else:
    print(f"\nBlock Heights are continuous from {min_block_height} to {max_block_height}.")

# List Missing Blocks (if any)
if total_expected_blocks != unique_hashes:
    missing_blocks = blocks_df[~blocks_df['height'].between(min_block_height, max_block_height)]['height'].compute()
    print("Missing Blocks:")
    print(missing_blocks)