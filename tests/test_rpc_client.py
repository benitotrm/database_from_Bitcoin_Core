import unittest
from src.api.rpc_client import RPCClient

class TestRPCClient(unittest.TestCase):
    def setUp(self):
        self.rpc_client = RPCClient()

    def test_rpc_call_batch(self):
        method = "getblockhash"
        param_list = [[100000], [200000], [300000]]
        responses = self.rpc_client.rpc_call_batch(method, param_list)
        
        for i, response in enumerate(responses):
            if response is not None:
                print(f"Successfully fetched data for block height {param_list[i][0]}: {response}")
            else:
                print(f"Failed to fetch data for block height {param_list[i][0]}")
        
        self.assertIsNotNone(responses[0])
        self.assertIsNotNone(responses[1])
        self.assertIsNotNone(responses[2])

if __name__ == '__main__':
    unittest.main()
