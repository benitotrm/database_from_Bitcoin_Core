#!/bin/bash

# Navigate to the repository directory
cd ~/Projects/database_from_Bitcoin_Core

# Define log file location with timestamp
LOGFILE=~/Projects/database_from_Bitcoin_Core/workflow_logs/workflow_$(date +"%Y%m%d_%H%M%S").log

# Define lock file location
LOCKFILE=/tmp/myjob.lock

# Check if lock file exists
if [ -e $LOCKFILE ]; then
  echo "Job is already running. Exiting..." | tee -a $LOGFILE
  exit 1
fi

# Create the lock file
touch $LOCKFILE

# Redirect stdout and stderr to the log file
exec > >(tee -a $LOGFILE) 2>&1

# Pull the latest changes from the repository
echo "Running git pull..."
git pull origin main

# Set PYTHONPATH to include the project's root directory
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Activate the virtual environment
echo "Activating virtual environment..."
source ~/Projects/database_from_Bitcoin_Core/venv/bin/activate

# Run the populate_blocks script
echo "Running populate_blocks.py..."
python -u src/blocks/populate_blocks.py

# Run the data quality checks
echo "Running blocks_dq.py..."
python -u src/blocks/blocks_dq.py

# Deactivate the virtual environment
echo "Deactivating virtual environment..."
deactivate

# Remove the lock file after the job is done
rm -f $LOCKFILE

# Log the completion time
echo "Workflow completed at $(date)"

