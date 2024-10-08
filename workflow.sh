#!/bin/bash

# Navigate to the repository directory
cd ~/Projects/database_from_Bitcoin_Core

# Define log file location with timestamp
LOGFILE=~/Projects/database_from_Bitcoin_Core/workflow_logs/workflow.log

# Redirect stdout and stderr to the log file
exec > >(tee $LOGFILE) 2>&1

# Log the current branch before switching
ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Current branch before switch: $ORIGINAL_BRANCH"

# Check for uncommitted changes before stashing
if ! git diff-index --quiet HEAD --; then
    echo "Stashing uncommitted changes..."
    git stash push -m "Auto stash before switching to main"
else
    echo "No local changes to save"
fi

# Switch to main branch
echo "Switching to main branch..."
git checkout main || { echo "Failed to switch to main branch"; git stash pop; exit 1; }

# Pull the latest changes from the repository
echo "Running git pull..."
git pull origin main || { echo "Failed to pull from main branch"; git checkout $ORIGINAL_BRANCH; git stash pop; exit 1; }

# Set PYTHONPATH to include the project's root directory
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Activate the virtual environment
echo "Activating virtual environment..."
source ~/Projects/database_from_Bitcoin_Core/venv/bin/activate

# Run the populate scripts
echo "Running population scripts..."
python -u src/blocks/populate_blocks.py
python -u src/transactions/populate_transactions.py

# Run the data quality checks
echo "Running DQ scripts..."
python -u src/blocks/blocks_dq.py
python -u src/transactions/transactions_dq.py

# Deactivate the virtual environment
echo "Deactivating virtual environment..."
deactivate

# Switch back to the original branch
echo "Switching back to original branch: $ORIGINAL_BRANCH"
git checkout $ORIGINAL_BRANCH || { echo "Failed to switch back to original branch"; git stash pop; exit 1; }

# Apply stashed changes if any were stashed
if git stash list | grep -q "Auto stash before switching to main"; then
    echo "Applying stashed changes..."
    git stash pop || { echo "Failed to apply stashed changes"; exit 1; }
else
    echo "No stashed changes to apply"
fi

# Log the completion time
echo "Workflow completed at $(date)"
