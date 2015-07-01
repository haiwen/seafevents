
import ccnet
from pysearpc import searpc_func

OFFICE_RPC_SERVICE_NAME = 'office-converter-rpcserver'

class OfficeConverterRpcClient(ccnet.RpcClientBase):
    def __init__(self, ccnet_client_pool, *args, **kwargs):
        ccnet.RpcClientBase.__init__(self, ccnet_client_pool, OFFICE_RPC_SERVICE_NAME,
                                     *args, **kwargs)

    @searpc_func("object", ["string", "string", "string"])
    def add_task(self, file_id, doctype, url):
        pass

    @searpc_func("object", ["string", "int"])
    def query_convert_status(self, file_id, page):
        pass
