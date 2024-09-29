#%% ###################### VOUT DQ ######################
###################### VOUT DQ ######################
pd.set_option('display.max_colwidth', 0)
# Load the necessary data
transactions_df = dd.read_parquet('_transactions_parquets/*.parquet', columns=['txid', 'is_coinbase'])
outputs_df = dd.read_parquet('_vout_parquets/*.parquet', columns=['txid'])
inputs_df = dd.read_parquet('_vin_parquets/*.parquet', columns=['txid'])

# Merge outputs with transactions to determine if they are coinbase or not
merged_df = dd.merge(outputs_df, transactions_df, on='txid', how='left')

# Filter out coinbase transactions as they don't need matching inputs
non_coinbase_outputs = merged_df[merged_df['is_coinbase'] == False]

# Check if non-coinbase outputs have corresponding inputs
# First, rename 'vin_txid' in inputs_df to 'txid' for merging
# inputs_df = inputs_df.rename(columns={'vin_txid': 'txid'})
# Perform the merge
final_merge = dd.merge(non_coinbase_outputs, inputs_df, on='txid', how='left', indicator=True)

# Find outputs that don't have matching inputs
unmatched_outputs = final_merge[final_merge['_merge'] == 'left_only'].compute()

if unmatched_outputs.empty:
    print("All non-coinbase outputs have corresponding inputs.")
else:
    print(f"Unmatched non-coinbase outputs: {unmatched_outputs}")
# %% ###################### SUMMARY ######################
     ###################### SUMMARY ######################
import dask.dataframe as dd
import os

# Function to summarize a dataset
def summarize_dataset(file_path):
    # Adjusting for CSV file
    if file_path.endswith('.csv'):
        df = dd.read_csv(file_path)
    else:
        df = dd.read_parquet(file_path if file_path.endswith('.parquet') else file_path + '*.parquet')
    
    # Extracting dataset name for display
    dataset_name = os.path.basename(os.path.normpath(file_path))
    
    print(f"Summary for {dataset_name}:")
    print("Data Types:")
    print(df.dtypes)
    
    if not df.columns.empty:  
        print("\nDescriptive Statistics:")
        descriptive_stats = df.describe().compute() 
        print(descriptive_stats)
    
    print("\nNumber of rows:", len(df))  
    print("-" * 50)

# List of datasets paths
datasets = ['_blocks.parquet', '_transactions_parquets/', '_vin_parquets/', '_vout_parquets/', 'address_behavior_dataset.csv']

for dataset in datasets:
    summarize_dataset(dataset)


# %% ###################### VISUALIZATION BASE ######################
     ###################### VISUALIZATION BASE ######################
import dask.dataframe as dd
import pandas as pd

# Load the datasets
blocks_df = dd.read_parquet('_blocks.parquet')
transactions_df = dd.read_parquet('_transactions_parquets/*.parquet')
outputs_df = dd.read_parquet('_vout_parquets/*.parquet')

# Add a 'date' column to blocks, converting 'time' to datetime
blocks_df['date'] = dd.to_datetime(blocks_df['time'], unit='s').dt.date

# Merge transactions with blocks to get the 'date'
transactions_df = transactions_df.merge(blocks_df[['block_hash', 'date']], on='block_hash', how='left')

# Now, merge outputs with transactions to carry over the 'date'
outputs_df = outputs_df.merge(transactions_df[['txid', 'date']], on='txid', how='left')

# Convert the 'addresses' lists into a count of addresses and add as a new column
outputs_df['address_count'] = outputs_df['addresses'].map(len, meta=('addresses', 'int'))

# Aggregate block counts by day
blocks_daily = blocks_df.groupby('date')['block_hash'].count().compute().reset_index()
blocks_daily = blocks_daily.rename(columns={'block_hash': 'block_count'})

# Aggregate transaction counts by day
transactions_daily = transactions_df.groupby('date')['txid'].count().compute().reset_index()
transactions_daily = transactions_daily.rename(columns={'txid': 'transaction_count'})

# Aggregate address counts by day
outputs_daily = outputs_df.groupby('date')['address_count'].sum().compute().reset_index()

# Preparing the daily summary DataFrame
# Convert 'date' to datetime to ensure alignment
blocks_daily['date'] = pd.to_datetime(blocks_daily['date'])
transactions_daily['date'] = pd.to_datetime(transactions_daily['date'])
outputs_daily['date'] = pd.to_datetime(outputs_daily['date'])

# Set 'date' as the index for each DataFrame
blocks_daily = blocks_daily.set_index('date')
transactions_daily = transactions_daily.set_index('date')
outputs_daily = outputs_daily.set_index('date')

# Concatenate DataFrames to create the daily summary
daily_summary_df = dd.concat([blocks_daily, transactions_daily, outputs_daily], axis=1).reset_index()

# Save the aggregated daily summary to a Parquet file
daily_summary_df.to_parquet('daily_summary', write_index=False)


# %% ###################### VISUALIZATION ######################
     ###################### VISUALIZATION ######################
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# Load the aggregated daily summary from the Parquet file
daily_summary_df = pd.read_parquet('daily_summary.parquet')

# Ensure 'date' is a datetime type, useful if not already converted
daily_summary_df['date'] = pd.to_datetime(daily_summary_df['date'])
daily_summary_df.set_index('date', inplace=True)

# Calculate the cumulative count of unique addresses
daily_summary_df['cumulative_address_count'] = daily_summary_df['address_count'].cumsum()

# Plotting Cumulative Unique Address Count by Date
plt.figure(figsize=(14, 7))
plt.plot(daily_summary_df.index, daily_summary_df['cumulative_address_count'], label='Cumulative Unique Address Count', color='tab:blue')
plt.xlabel('Date')
plt.ylabel('Cumulative Unique Address Count')

# Format the y-axis to show numbers in millions with 'M' suffix
plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: '{:,.0f}M'.format(x/1e6)))

plt.title('Cumulative Unique Address Count by Date')
plt.legend()
plt.grid(True, which="both", ls="--", linewidth=0.5)
plt.show()

# Plotting Block count and Transaction count by date on dual axis with formatted y-axis
fig, ax1 = plt.subplots(figsize=(14, 7))

color = 'tab:red'
ax1.set_xlabel('Date')
ax1.set_ylabel('Block Count', color=color)
ax1.plot(daily_summary_df.index, daily_summary_df['block_count'], color=color)
ax1.tick_params(axis='y', labelcolor=color)

# Format the y-axis to show numbers with 'K' suffix for thousands
ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}K'.format(x/1000)))

ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
color = 'tab:blue'
ax2.set_ylabel('Transaction Count (in thousands)', color=color)
ax2.plot(daily_summary_df.index, daily_summary_df['transaction_count'], color=color)
ax2.tick_params(axis='y', labelcolor=color)

# Format the y-axis to show numbers with 'K' suffix for thousands
ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}K'.format(x/1000)))

fig.tight_layout()  # otherwise the right y-label is slightly clipped
plt.title('Block Count and Transaction Count by Date')
plt.show()
