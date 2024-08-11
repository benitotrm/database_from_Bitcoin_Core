# database_from_Bitcoin_Core

## 1. Setup 

This repository assumes you're alredy running a full bitcoin node on your machine and have properly configured the RCP API. If this is not the case please follow these instructions first: https://bitcoin.org/en/full-node

Once your node is fully functional you can start using this repo to generate your database. 

Go to /src/api/rpc_client.py and set `rpc_user` and `rpc_password` with your actual credentials.

We strongly recommend you set these variables permanently. For this doing this on Linux, add them to your shell configuration file (e.g., `~/.bashrc`, `~/.zshrc`) with names `RPC_USER` and `RPC_PASSWORD` and reload the configuration. For doing this on Windows you have to set them up as environment variables.

## 2. Testing

I'ts important you run the following unit test first, as they are focused on ensuring the API is properly set up. 

### Running Unit Tests

This project uses Python's built-in `unittest` framework to run unit tests and ensure that the code functions as expected.

To run all the tests in the project, use the following command:

```bash
python -m unittest discover
```
What This Command Does:
- **`-m unittest`**: Tells Python to run the `unittest` module as a script. This module is Python's built-in testing framework, which discovers and runs tests.
- **`discover`**: Instructs `unittest` to automatically discover all test files and execute them. It looks for files that match the pattern `test*.py` (e.g., `test_module.py`, `test_rpc_client.py`) in the project directory.

This command will:

1. Automatically find and execute all test files in the `tests/` directory.
2. Report on the success or failure of each test, providing detailed information on any errors encountered.

Replace `your_rpc_user` and `your_rpc_password` with your actual credentials.

If you prefer to set these variables permanently, add them to your shell configuration file (e.g., `~/.bashrc`, `~/.zshrc`) and reload the configuration.
