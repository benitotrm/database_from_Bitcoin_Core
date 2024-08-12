# database_from_Bitcoin_Core

## RPC API setup 

This repository assumes you're alredy running a full bitcoin node on your machine and have properly configured the RCP API. If this is not the case please follow these instructions first: https://bitcoin.org/en/full-node. Once your node is fully functional you can start using this repo to generate your database. 

Go to /src/api/rpc_client.py and set `rpc_user` and `rpc_password` with your actual credentials. We strongly recommend you set these variables globally and not to hardcode them. For this doing this on Linux, add them to your shell configuration file (e.g., `~/.bashrc`, `~/.zshrc`) with names `RPC_USER` and `RPC_PASSWORD` and reload the configuration. For doing this on Windows you have to set them up as environment variables.

I'ts important you run the following unit test first, as they are focused on ensuring the API is properly set up. 

```bash
python -m unittest discover
```

## ETL process
The following commands execute the complete population of each dataset:

```bash
python src/populate_blocks.py 
```
Same commands with an example use of their optional parameters:

```bash
# Selecting 'start' and 'end' block height
python src/populate_blocks.py --start 10000 --end 20000 
```

## DQ process
The following are the commands execut the relevant Data Quality checks of each dataset:

```bash
python src/blocks_dq.py 
```
