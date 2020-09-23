import ACI
import time

conn = ACI.create(ACI.Client, 8675, "127.0.0.1")

while conn.ws == 0:
    pass

conn.authenticate("term.jordan", "AbDc314")

def test(cmd):
    print(cmd["data"])

conn.add_event_callback(ACI.event_callback("test_event", test))

conn.send_event("term.jordan", "test_event", "Hello")

print(conn["config"]["ip"])

#conn["db1"].set_value_noack("val", "Greetings")
#print(conn["minecraft"]["stop"])