import ACI

conn = ACI.create(ACI.Client, 8675, "127.0.0.1")

while conn.ws == 0:
    pass

conn["db1"]["val"] = "Hello World!"
print(conn["db1"]["val"])
