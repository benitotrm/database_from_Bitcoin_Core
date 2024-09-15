'''Common functions used across modules.'''
import os
import glob
import errno
import shutil
import subprocess
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

def consolidate_parquet_files(input_directory, output_directory, target_partition_size="1GB", batch_size=1000, reprocess=False):
    '''Consolidates Parquet files in the input directory into the output directory'''
    # Ensure output_directory exists
    os.makedirs(output_directory, exist_ok=True)
    
    # Get list of new Parquet files
    new_parquet_files = glob.glob(os.path.join(input_directory, '*.parquet'))
    new_parquet_files.sort()
    
    # Get existing Parquet files
    existing_parquet_files = glob.glob(os.path.join(output_directory, '*.parquet'))
    existing_parquet_files.sort()

    # Determine the starting point for numbering
    if existing_parquet_files:
        last_file = existing_parquet_files[-1]
        last_number = int(last_file.split('.')[-2])  # Extract the file number
        file_counter = last_number + 1
    else:
        file_counter = 0
    
    # If reprocessing, include existing files and adjust batch size
    if reprocess:
        new_parquet_files = existing_parquet_files + new_parquet_files
        batch_size = 10  # Set smaller batch size for reprocessing
        file_counter = 0  # Start numbering from scratch when reprocessing

    # Process files in batches
    total_files = len(new_parquet_files)
    num_batches = (total_files + batch_size - 1) // batch_size

    for batch_num in range(num_batches):
        batch_start = batch_num * batch_size
        batch_end = min((batch_num + 1) * batch_size, total_files)
        batch_files = new_parquet_files[batch_start:batch_end]
        
        print(f"Processing batch {batch_num + 1}/{num_batches} with files {batch_start} to {batch_end - 1}")
        
        # Read batch of files
        ddf = dd.read_parquet(batch_files, engine='pyarrow', ignore_divisions=True)
        
        # Repartition to desired partition size
        ddf = ddf.repartition(partition_size=target_partition_size)
        
        # Define name function to continue numbering from existing files
        def name_function(i):
            return f"part.{file_counter + i}.parquet"
        
        # Write the batch to Parquet
        ddf.to_parquet(
            output_directory,
            name_function=name_function,
            write_index=False,
            append=True,
            overwrite=False,
            engine='pyarrow'
        )
        
        # Update file counter
        file_counter += ddf.npartitions

    print(f"Consolidation complete. New files written to {output_directory}")

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
