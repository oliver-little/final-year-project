from client.multiprocessing import managers
from client.model.table import *
from client.rpc.client_service import InsecureClientQueryManager

table = Table()
newT = table.filter(F("col1") < F("col2"), Function("Left", F("col1")).contains("hello"))

manager = InsecureClientQueryManager("localhost:50051").open()
manager.send_table(newT.to_protobuf())
manager.close()