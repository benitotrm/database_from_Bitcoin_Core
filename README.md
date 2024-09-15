# database_from_Bitcoin_Core

This guide is focused on Linux, but all steps should be replicable on Windows.

## Bitcoin Core setup

This guide assumes you're alredy running a full bitcoin node on your machine and have properly configured the RCP API. If this is not the case please follow these instructions first: https://bitcoin.org/en/full-node. Once your node is fully synced you can start using this repo to generate your database. 

## Python environment setup

Setting up a virual environment:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -e . 
```
And you can use 'deactivate' on the bash when done.

You need to set your RPC API credentials `rpc_user` and `rpc_password` for the code to work. Use the python-dotenv library and add them to a .env file on your root folder like this:

```bash
RPC_USER=your_rpc_username
RPC_PASSWORD=your_rpc_password
```

Then, it's important you run the following unit test first, as they are focused on ensuring the API is properly set up. 

```bash
python -m unittest discover
```

## ETL process
The following commands execute the complete population of each dataset:

```bash
python src/blocks/populate_blocks.py
python src/transactions/populate_transactions.py
...
```
Same commands with an example use of their optional parameters:

```bash
# Selecting 'start' and 'end' block height
python src/populate_blocks.py --start 10000 --end 20000 
```

## DQ process
The following are the commands execut the relevant Data Quality checks of each dataset:

```bash
python src/blocks/blocks_dq.py
python src/transactions/transacitions_dq.py 
...
```

## Automation workflow

You can run these files manually or setup an automated workflow using cron that can run automatically or manually. 

For the cron job to work seamlessly I prefer to use SSH. I set the agent to start on each machine reboot so it's properly configured to update the agent, environment and key paths by adding this configuration on ~/.bashrc.

Cron can be setup as follows:

```bash
crontab -e
```
Line to add to cron for a scheduled midnight run:
```bash
0 0 * * * ~/Projects/database_from_Bitcoin_Core/workflow.sh
```

Example manual run:
```bash
~/Projects/database_from_Bitcoin_Core/workflow.sh
```