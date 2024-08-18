'''Module to connect to the Bitcoin Core RPC API'''
import os
import json
import time
import requests

class RPCClient:
    '''RPC API Client object generator.'''
    def __init__(self, rpc_user=None, rpc_password=None, rpc_host="127.0.0.1", rpc_port="8332"):
        self.rpc_user = rpc_user or os.environ.get('RPC_USER')
        self.rpc_password = rpc_password or os.environ.get('RPC_PASSWORD')
        if not self.rpc_user or not self.rpc_password:
            raise EnvironmentError("RPC_USER and/or RPC_PASSWORD environment variables not set.")
        
        self.rpc_url = f"http://{self.rpc_user}:{self.rpc_password}@{rpc_host}:{rpc_port}"
        self.headers = {'content-type': 'text/plain'}
        self.max_retries = 3

    def rpc_call_batch(self, method, param_list):
        '''Creates a batch of calls to the RPC API, sends it and retrieve the information.'''
        # responses = []
        payload = json.dumps([{
            "jsonrpc": "2.0", 
            "id": str(index), 
            "method": method, 
            "params": params
        } for index, params in enumerate(param_list)])
        
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.post(self.rpc_url, headers=self.headers, data=payload, timeout=10)
                response.raise_for_status()
                batch_response = response.json()
                
                if all('error' not in resp or resp['error'] is None for resp in batch_response):
                    return batch_response
                else:
                    errors = [resp['error'] for resp in batch_response if 'error' in resp and resp['error']]
                    print(f"RPC Batch Error at attempt {attempt}: {errors}")
            
            except (requests.exceptions.HTTPError, requests.exceptions.RequestException, json.JSONDecodeError) as e:
                print(f"RPC Batch Attempt {attempt} failed with error: {e}")
                if attempt < self.max_retries:
                    time.sleep(5)  # Implement exponential backoff if needed
        
        return [None] * len(param_list)
