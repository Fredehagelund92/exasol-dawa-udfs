CREATE OR REPLACE PYTHON3 SET SCRIPT ETL_UDS.dawa_txid (...)
returns INTEGER AS


import sys
import glob

sys.path.extend(
	glob.glob('/buckets/bfsdefault/default/dawa-sdk-0.0.2/dawa-sdk-0.0.2')
)

from dawa import API

def run(ctx):

	api = API()
	
	txid = api.txid()
	
	if txid is None:
		raise ValueError('dawa api returned a empty txId')


	return api.txid()

/