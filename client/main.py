from model.table import *

table = Table()
newT = table.filter(F("col1") < F("col2"), Function("Left", F("col1")).contains("hello"))
print(newT)