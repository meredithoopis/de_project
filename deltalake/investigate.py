from deltalake import DeltaTable 

print("*" * 80)
table = DeltaTable("data/taxi-data/")

print("[INFO] Table loaded successfully")


print("*"*80)
print("[INFO] Delta Lake table schema:")
print(table.schema().json())
print("[INFO] Delta Lake table version:")
print(table.version())
print("[INFO] Delta Lake metadata:")
print(table.metadata())

print("*"*80)
print("[INFO] Delta Lake table data on pressure column:")
print(table.to_pandas(columns=["pressure"]))

print("*"*80)
print("[INFO] History of actions performed on the table")
print(table.history())