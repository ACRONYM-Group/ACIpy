import ACI
import time

ACI.create(ACI.Server, port=8765)

while True:
    time.sleep(1)
