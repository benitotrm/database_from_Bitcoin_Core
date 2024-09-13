'''Common functions used across modules.'''
import os
import errno
import shutil
import tempfile
import subprocess
import pandas as pd
import dask.dataframe as dd

def get_current_branch():
    '''Returns the name of the current branch'''
    try:
        # Get the current branch name
        branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], universal_newlines=True).strip()
        return branch
    except subprocess.CalledProcessError:
        # Handle case where git command fails (e.g., not in a git repo)
        return 'unknown'

def get_max_block_height_on_file(env):
    """Returns the maximum block height available in the existing Parquet files"""
    blocks_directory = f'database/blocks_{env}'
    if os.path.exists(blocks_directory):
        parquet_files = [
            os.path.join(blocks_directory, f)
            for f in os.listdir(blocks_directory)
            if f.endswith(".parquet")
            ]
        if parquet_files:
            existing_blocks_df = dd.read_parquet(parquet_files)
            if existing_blocks_df.shape[0].compute() > 0:
                return existing_blocks_df['height'].max().compute()
    return None

def consolidate_parquet_files(input_directory, output_directory):
    """Consolidates small parquet files into larger files of approximately 1GB"""
    
    # Create a temporary directory for writing consolidated files
    temp_output_directory = tempfile.mkdtemp()

    # Load existing legacy data from the consolidated directory if it exists
    if os.path.exists(output_directory) and os.listdir(output_directory):
        existing_df = dd.read_parquet(f'{output_directory}/*.parquet', index=False)
    else:
        existing_df = None

    # Load new Parquet files
    try:
        new_df = dd.read_parquet(f'{input_directory}/*.parquet', index=False)
    except FileNotFoundError:
        print(f"No new Parquet files found in {input_directory}. Proceeding with existing data.")
        new_df = dd.from_pandas(pd.DataFrame(), npartitions=1)

    # Combine existing and new data
    if existing_df is not None:
        combined_df = dd.concat([existing_df, new_df], interleave_partitions=True)
    else:
        combined_df = new_df

    # Reset index to ensure proper partitioning
    combined_df = combined_df.reset_index(drop=True)

    # If the combined dataframe is empty, return without doing anything
    if combined_df.shape[0].compute() == 0:
        print("No data to consolidate.")
        return

    # Repartition the dataframe to have partitions of approximately 2GB
    combined_df = combined_df.repartition(partition_size="2GB")

    # Write the combined data back to the consolidated output directory
    combined_df.to_parquet(temp_output_directory, write_metadata_file=False)

    # Replace the old output directory with the new data
    shutil.rmtree(output_directory)
    shutil.move(temp_output_directory, output_directory)

    print(f"Consolidated Parquet files written to {output_directory}")

def delete_unconsolidated_directory(input_directory, consolidated_output_directory):
    """Deletes the entire input directory if the consolidation was successful"""
    if os.path.exists(consolidated_output_directory) and os.listdir(consolidated_output_directory):
        print(f"Preparing to erase {input_directory} from existence...")
        try:
            shutil.rmtree(input_directory)
            print(f"{input_directory} has been vaporized.")
        except PermissionError as pe:
            print(f"Premission denied while trying to delete {input_directory}. Error: {pe}")
        except OSError as oe:
            if oe.errno == errno.ENOENT:
                print(f"Directory {input_directory} doesn't exist, so it's already gone!")
            elif oe.errno == errno.EACCES:
                print(f"Access denied when trying to delete {input_directory}. Error: {oe}")
            else:
                print(f"An OS error ocurred while deleting {input_directory}. Error: {oe}")
    else:
        print("Consolidation not confirmed. Directory spared for now.")
