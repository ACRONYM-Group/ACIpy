import asyncio
import websockets
import json
import time
from queue import SimpleQueue

try:
    from utils import allow_sync
except Exception:
    from ACIpy.utils import allow_sync


ACIVersion = "2020.07.14.1"

connections = {}


async def _recv_handler(websocket, _, responses):
    """
    Handles a Server response

    :param websocket:
    :param _:
    :param responses:
    :return:
    """
    raw = await websocket.recv()
    cmd = json.loads(raw)

    if cmd["cmd"] == "getResp":
        value = json.dumps({"cmd_typ": "get_val", "key":cmd["key"], "db_key": cmd["db_key"], "val": cmd["val"]})
        responses.put(value)

    if cmd["cmd"] == "setResp":
        value = json.dumps({"cmd_typ": "set_val", "val": cmd["msg"]})
        responses.put(value)

    if cmd["cmd"] == "ldResp":
        value = json.dumps({"cmd_typ": "ld", "val": cmd["msg"]})
        responses.put(value)

    if cmd["cmd"] == "a_auth_response":
        value = json.dumps({"cmd_typ": "auth_msg", "val": cmd["msg"]})
        responses.put(value)

    if cmd["cmd"] == "get_indexResp":
        value = json.dumps({"cmd_typ": "get_indexResp", "val": cmd["msg"]})
        responses.put(value)

    if cmd["cmd"] == "set_indexResp":
        value = json.dumps({"cmd_typ": "set_indexResp", "val": cmd["msg"]})
        responses.put(value)

    if cmd["cmd"] == "app_indexResp":
        value = json.dumps({"cmd_typ": "app_indexResp", "val": cmd["msg"]})
        responses.put(value)

    if cmd["cmd"] == "get_len_indexResp":
        value = json.dumps({"cmd_typ": "get_len_indexResp", "val": cmd["msg"]})
        responses.put(value)

    if cmd["cmd"] == "get_recent_indexResp":
        value = json.dumps({"cmd_typ":"get_recent_indexResp", "val": cmd["msg"]})
        responses.put(value)

    if cmd["cmd"] == "event":
        for index in connections:
            for callback_index in connections[index].event_callbacks:
                if callback_index.event_id == cmd["event_id"]:
                    callback_index.function(cmd)
        


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
        await self.conn.ws.send(json.dumps({"cmd": "wtd", "db_key": self.db_key}))

    @allow_sync
    async def read_from_disk(self):
        """
        Read Database data from disk

        :return:
        """
        await self.conn.ws.send(json.dumps({"cmd": "rfd", "db_key": self.db_key}))

    @allow_sync
    async def list_databases(self):
        """
        Get a list of all connected databases

        :return:
        """
        await self.conn.ws.send(json.dumps({"cmd": "list_databases", "db_key": self.db_key}))
        return json.loads(await self.conn.wait_for_response("ld", None, self.db_key))

    async def _get_value(self, key):
        await self.conn.ws.send(json.dumps({"cmd": "get_val", "key": key, "db_key": self.db_key}))
        response = await self.conn.wait_for_response("get_val", key, self.db_key, cmd_type="get_val")
        return response

    @allow_sync
    async def set_value(self, key, val):
        await self.conn.ws.send(json.dumps({"cmd": "set_val", "key": key, "db_key": self.db_key, "val": val}))
        response = await self.conn.wait_for_response(cmd_type="set_val")
        return response

    @allow_sync
    async def set_value_noack(self, key, val):
        await self.conn.ws.send(json.dumps({"cmd": "set_val", "key": key, "db_key": self.db_key, "val": val}))
        return "no ack"

    @allow_sync
    async def get_value(self, key):
        return await self._get_value(key)

    @allow_sync
    async def get_index(self, key, index):
        await self.conn.ws.send(json.dumps({"cmd": "get_index", "key": key, "db_key": self.db_key, "index":index}))
        response = await self.conn.wait_for_response("get_indexResp", key, self.db_key)
        return response

    @allow_sync
    async def set_index(self, key, index, value):
        await self.conn.ws.send(json.dumps({"cmd": "set_index", "key": key, "db_key": self.db_key, "index":index, "value": value}))
        response = await self.conn.wait_for_response("set_indexResp", key, self.db_key)
        return response

    @allow_sync
    async def set_index_noack(self, key, index, value):
        await self.conn.ws.send(json.dumps({"cmd": "set_index", "key": key, "db_key": self.db_key, "index":index, "value": value}))
        return "no ack"

    @allow_sync
    async def append_index(self, key, value):
        await self.conn.ws.send(json.dumps({"cmd": "append_index", "key": key, "db_key": self.db_key, "value": value}))
        response = await self.conn.wait_for_response("app_indexResp", key, self.db_key)
        return response

    @allow_sync
    async def append_index_noack(self, key, value):
        await self.conn.ws.send(json.dumps({"cmd": "append_index", "key": key, "db_key": self.db_key, "value": value}))
        return "no ack"

    @allow_sync
    async def get_len_index(self, key):
        await self.conn.ws.send(json.dumps({"cmd": "get_len_index", "key": key, "db_key": self.db_key}))
        response = await self.conn.wait_for_response("get_len_indexResp", key, self.db_key)
        return response

    @allow_sync
    async def get_recent_index(self, key, num):
        await self.conn.ws.send(json.dumps({"cmd": "get_recent_index", "key": key, "db_key": self.db_key, "num":num}))
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


class event_callback:
    def __init__(self, event_id, callback_function):
        self.event_id = event_id
        self.function = callback_function

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
        self.id = "not-authed"

        self.interfaces = {}
        self.event_callbacks = []

        connections[name] = self

    async def start(self):
        await self._create(self.port, self.ip, self.loop, self.responses)

    async def wait_for_response(self, _, key="none", db_key="none", cmd_type="any"):
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
                if cmd["cmd_typ"] == cmd_type or cmd_type == "any":

                    if cmd["cmd_typ"] == "get_val" and cmd["key"] == key and cmd["db_key"] == db_key:
                        return cmd["val"]
                    elif cmd["cmd_typ"] == "set_val":
                        return cmd["val"]
                    elif cmd["cmd_typ"] == "ld":
                        return cmd["val"]
                    elif cmd["cmd_typ"] == "auth_msg":
                        return cmd["val"]
                    elif cmd["cmd_typ"] == "get_indexResp" and cmd["key"] == key and cmd["db_key"] == db_key:
                        return cmd["val"]
                    elif cmd["cmd_typ"] == "set_indexResp" and cmd["key"] == key and cmd["db_key"] == db_key:
                        return cmd["val"]
                    elif cmd["cmd_typ"] == "app_indexResp" and cmd["key"] == key and cmd["db_key"] == db_key:
                        return cmd["val"]
                    elif cmd["cmd_typ"] == "get_len_indexResp" and cmd["key"] == key and cmd["db_key"] == db_key:
                        return cmd["val"]
                    elif cmd["cmd_typ"] == "get_recent_indexResp" and cmd["key"] == key and cmd["db_key"] == db_key:
                        return cmd["val"]

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
        await self.ws.send(json.dumps({"cmd": "cdb", "db_key": db_key}))

    @allow_sync
    async def authenticate(self, id, token):
        self.id = id
        await self.ws.send(json.dumps({"cmd":"a_auth", "id":id, "token":token}))
        return await self.wait_for_response("auth_msg", None, None)

    @allow_sync
    async def send_event(self, destination, event_id, data):
        await self.ws.send(json.dumps({"cmd":"event", "event_id":event_id, "destination": destination, "data": data, "origin":self.id}))

    def add_event_callback(self, event_callback):
        self.event_callbacks.append(event_callback)
