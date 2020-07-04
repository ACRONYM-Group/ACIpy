import asyncio
import websockets
import json
import time
from queue import SimpleQueue

try:
    from utils import allow_sync
except Exception:
    from ACI.utils import allow_sync


ACIVersion = "2020.07.01.1"

connections = {}


async def _recv_handler(websocket, _, responses):
    """
    Handles a Server response

    :param websocket:
    :param _:
    :param responses:
    :return:
    """
    cmd = json.loads(await websocket.recv())
    print(cmd)

    if cmd["cmdType"] == "getResp":
        value = json.dumps(["get_val", cmd["key"], cmd["db_key"], cmd["val"]])
        responses.put(value)

    if cmd["cmdType"] == "setResp":
        value = json.dumps(["set_val", cmd["msg"]])
        responses.put(value)

    if cmd["cmdType"] == "ldResp":
        value = json.dumps(["ld", cmd["msg"]])
        responses.put(value)

    if cmd["cmdType"] == "a_auth_response":
        value = json.dumps(["auth_msg", cmd["msg"]])
        responses.put(value)

    if cmd["cmdType"] == "get_indexResp":
        value = json.dumps(["get_indexResp", cmd["msg"]])
        responses.put(value)

    if cmd["cmdType"] == "set_indexResp":
        value = json.dumps(["set_indexResp", cmd["msg"]])
        responses.put(value)

    if cmd["cmdType"] == "app_indexResp":
        value = json.dumps(["app_indexResp", cmd["msg"]])
        responses.put(value)

    if cmd["cmdType"] == "get_len_indexResp":
        value = json.dumps(["get_len_indexResp", cmd["msg"]])
        responses.put(value)

    if cmd["cmdType"] == "get_recent_indexResp":
        value = json.dumps(["get_recent_indexResp", cmd["msg"]])
        responses.put(value)
        


class ContextualDatabaseInterface:
    def __init__(self, interface):
        self._interface = interface
        self.conn = interface.conn
        self.db_key = interface.db_key

        self.record = {}

    def __getitem__(self, item):
        if item in self._record:
            return self._record[item]
        return self.interface[item]

    def __setitem__(self, item, val):
        self.record[item] = val

    @allow_sync
    async def set_item(self, key, val):
        self[key] = val

    @allow_sync
    async def get_item(self, key):
        return self[key]

    @allow_sync
    async def list_databases(self):
        return await self.conn.list_databases()

    @allow_sync
    async def read_from_disk(self):
        await self.conn.read_from_disk()

    @allow_sync
    async def write_to_disk(self):
        await self.conn.write_to_disk()


class DatabaseInterface:
    """
        ACI Database Interface
    """
    def __init__(self, connection, db_key):
        self.conn = connection
        self.db_key = db_key

        self._contextual = None

    @allow_sync
    async def write_to_disk(self):
        """
        Write Database data to disk

        :return:
        """
        await self.conn.ws.send(json.dumps({"cmdType": "wtd", "db_key": self.db_key}))

    @allow_sync
    async def read_from_disk(self):
        """
        Read Database data from disk

        :return:
        """
        await self.conn.ws.send(json.dumps({"cmdType": "rfd", "db_key": self.db_key}))

    @allow_sync
    async def list_databases(self):
        """
        Get a list of all connected databases

        :return:
        """
        await self.conn.ws.send(json.dumps({"cmdType": "list_databases", "db_key": self.db_key}))
        return json.loads(await self.conn.wait_for_response("ld", None, self.db_key))

    async def _get_value(self, key):
        await self.conn.ws.send(json.dumps({"cmdType": "get_val", "key": key, "db_key": self.db_key}))
        response = await self.conn.wait_for_response("get_val", key, self.db_key)
        return response

    @allow_sync
    async def set_value(self, key, val):
        await self.conn.ws.send(json.dumps({"cmdType": "set_val", "key": key, "db_key": self.db_key, "val": val}))
        response = await self.conn.wait_for_response("set_val")
        return response

    @allow_sync
    async def get_value(self, key):
        return await self._get_value(key)

    @allow_sync
    async def get_index(self, key, index):
        await self.conn.ws.send(json.dumps({"cmdType": "get_index", "key": key, "db_key": self.db_key, "index":index}))
        response = await self.conn.wait_for_response("get_indexResp", key, self.db_key)
        return response

    @allow_sync
    async def set_index(self, key, index, value):
        await self.conn.ws.send(json.dumps({"cmdType": "set_index", "key": key, "db_key": self.db_key, "index":index, "value": value}))
        response = await self.conn.wait_for_response("set_indexResp", key, self.db_key)
        return response

    @allow_sync
    async def append_index(self, key, value):
        await self.conn.ws.send(json.dumps({"cmdType": "append_index", "key": key, "db_key": self.db_key, "value": value}))
        response = await self.conn.wait_for_response("app_indexResp", key, self.db_key)
        return response

    @allow_sync
    async def get_len_index(self, key):
        await self.conn.ws.send(json.dumps({"cmdType": "get_len_index", "key": key, "db_key": self.db_key}))
        response = await self.conn.wait_for_response("get_len_indexResp", key, self.db_key)
        return response

    @allow_sync
    async def get_recent_index(self, key, num):
        await self.conn.ws.send(json.dumps({"cmdType": "get_recent_index", "key": key, "db_key": self.db_key, "num":num}))
        response = await self.conn.wait_for_response("get_recent_indexResp", key, self.db_key)
        return response   

    def __getitem__(self, key):
        return self.get_value(key)

    def __setitem__(self, key, val):
        self.set_value(key, val)

    async def __aenter__(self):
        self._contextual = ContextualDatabaseInterface(self)
        return self._contextual

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for key in self._contextual.record:
            await self.set_value(key, self._contextual.record[key])


class Connection:
    """
        ACI Connection
    """
    def __init__(self, loop, ip, port, name):
        """
        :param ip:
        :param port:
        :param loop:
        """
        global connections

        self.ip = ip
        self.port = port
        self.ws = 0
        self.responses = SimpleQueue()
        self.loop = loop
        self.name = name

        self.interfaces = {}

        connections[name] = self

    async def start(self):
        await self._create(self.port, self.ip, self.loop, self.responses)

    async def wait_for_response(self, _, key="none", db_key="none"):
        """
        Waits for a response
        :param _:
        :param key:
        :param db_key:
        :return:
        """
        while True:
            if not self.responses.empty():
                value = self.responses.get_nowait()
                cmd = json.loads(value)
                print(cmd)
                if tuple(cmd)[:3] == ("get_val", key, db_key):
                    return cmd[3]
                elif cmd[0] == ("set_val"):
                    print(cmd[1])
                    return cmd[1]
                elif cmd[0] == "ld":
                    return cmd[1]
                elif cmd[0] == "auth_msg":
                    return cmd[1]
                elif cmd[0] == "get_indexResp":
                    return cmd[1]
                elif cmd[0] == "set_indexResp":
                    return cmd[1]
                elif cmd[0] == "app_indexResp":
                    return cmd[1]
                elif cmd[0] == "get_len_indexResp":
                    return cmd[1]
                elif cmd[0] == "get_recent_indexResp":
                    return cmd[1]

    async def _create(self, port, ip, loop, responses):
        """
        Initializes the connection

        :param port:
        :param ip:
        :param loop:
        :param responses:
        :return:
        """
        await self.handler(loop, responses, ip, port)

    async def handler(self, loop, responses, ip="127.0.0.1", port=8765):
        """
        Creates a handler

        :param loop:
        :param responses:
        :param ip:
        :param port:
        :return:
        """
        asyncio.set_event_loop(loop)
        uri = "ws://%s:%s" % (ip, port)

        async with websockets.connect(uri) as websocket:
            # print(websocket)
            self.ws = websocket
            time.sleep(0.25)
            while True:
                consumer_task = asyncio.ensure_future(_recv_handler(self.ws, uri, responses))

                done, pending = await asyncio.wait([consumer_task], return_when=asyncio.FIRST_COMPLETED)

                for task in pending:
                    task.cancel()

    def _get_interface(self, database_key):
        """
        Gets an interface to the Database with the given keys

        :param database_key:
        :return:
        """
        return DatabaseInterface(self, database_key)

    def __getitem__(self, key):
        if key not in self.interfaces:
            self.interfaces[key] = self._get_interface(key)
        return self.interfaces[key]

    async def create_database(self, db_key):
        await self.ws.send(json.dumps({"cmdType": "cdb", "db_key": db_key}))

    @allow_sync
    async def authenticate(self, id, token):
        await self.ws.send(json.dumps({"cmdType":"a_auth", "id":id, "token":token}))
        return await self.wait_for_response("auth_msg", None, None)
