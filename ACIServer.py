import websockets
import asyncio
import json
import traceback
import requests
import random
from google.oauth2 import id_token
from google.auth.transport import requests

try:
    from database import Database
except Exception as e:
    tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
    print(tb_str)
    from ACI.database import Database

ACIVersion = "2020.07.01.1"

class ServerClient:
    def __init__(self, clientID, user_type, clientWebsocket, user_id):
        self.id = clientID
        self.websocket = clientWebsocket
        self.user_type = user_type
        self.user_id = user_id


class Server:
    def __init__(self, loop, ip="localhost", port=8765, _=""):
        self.ip = ip
        self.port = port
        self.dbs = {}
        self.clients = []
        self.loop = loop
        self.rootDir = "./"

    def start(self):
        """
        Starts the server running
        :return:
        """
        asyncio.set_event_loop(self.loop)
        self.load_config()
        start_server = websockets.serve(self.connection_handler, self.ip, self.port)

        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()

    async def connection_handler(self, websocket, path):
        websocket.user = "NotAuthed"
        while True:
            raw_cmd = await websocket.recv()
            cmd = json.loads(raw_cmd)
            response = ""

            clientIndex = "NotFound"
            for index, Client in enumerate(self.clients):
                if Client.user_type + Client.user_id == websocket.user:
                    clientIndex = index

            if clientIndex != "NotFound":
                user = self.clients[clientIndex]
            else:
                user = "NotAuthed"


            if cmd["cmdType"] == "get_val":
                response = self.get_response_packet(cmd["key"], cmd["db_key"], websocket.user)
                await websocket.send(response)
            
            if cmd["cmdType"] == "set_val":
                newValue = self.dbs[cmd["db_key"]].set(cmd["key"], cmd["val"], websocket.user)
                response = json.dumps({"cmdType": "setResp", "msg": str(cmd["db_key"]) + "[" + str(cmd["key"]) + "] = " + str(newValue)})
                await websocket.send(response)

            if cmd["cmdType"] == "get_index":
                response = json.dumps({"cmdType": "get_indexResp", "msg": self.dbs[cmd["db_key"]].data[cmd["key"]].get_index(cmd["index"], websocket.user)})
                await websocket.send(response)

            if cmd["cmdType"] == "set_index":
                response = json.dumps({"cmdType": "set_indexResp", "msg": self.dbs[cmd["db_key"]].data[cmd["key"]].set_index(cmd["index"], cmd["value"], websocket.user)})
                await websocket.send(response)

            if cmd["cmdType"] == "append_index":
                response = json.dumps({"cmdType": "app_indexResp", "msg": self.dbs[cmd["db_key"]].data[cmd["key"]].append_index(cmd["value"], websocket.user)})
                await websocket.send(response)

            if cmd["cmdType"] == "get_len_index":
                response = json.dumps({"cmdType": "get_len_indexResp", "msg": self.dbs[cmd["db_key"]].data[cmd["key"]].get_len(websocket.user)})
                await websocket.send(response)

            if cmd["cmdType"] == "get_recent_index":
                print("Client is requesting recent index")
                response = json.dumps({"cmdType": "get_recent_indexResp", "msg": self.dbs[cmd["db_key"]].data[cmd["key"]].get_recent(cmd["num"], websocket.user)})
                await websocket.send(response)
                print("sent")

            if cmd["cmdType"] == "wtd":
                self.write_to_disk(cmd["db_key"])

            if cmd["cmdType"] == "rfd":
                self.read_from_disk(cmd["db_key"])

            if (cmd["cmdType"] == "cdb"):
                self.dbs[cmd["db_key"]] = Database(cmd["db_key"], read=False, root_dir=self.rootDir)
                
            if cmd["cmdType"] == "list_databases":
                response = json.dumps({"cmdType": "ldResp",
                                       "msg": json.dumps(list(self.dbs[cmd["db_key"]].data.keys()))})
                await websocket.send(response)

            if cmd["cmdType"] == "g_auth":
                print("Starting Google Auth")
                try:
                    token = cmd["id_token"]
                    # Specify the CLIENT_ID of the app that accesses the backend:
                    idinfo = id_token.verify_oauth2_token(token, requests.Request())

                    # Or, if multiple clients access the backend server:
                    # idinfo = id_token.verify_oauth2_token(token, requests.Request())
                    # if idinfo['aud'] not in [CLIENT_ID_1, CLIENT_ID_2, CLIENT_ID_3]:
                    #     raise ValueError('Could not verify audience.')

                    if idinfo['iss'] not in ['accounts.google.com', 'https://accounts.google.com']:
                        raise ValueError('Wrong issuer.')

                    # If auth request is from a G Suite domain:
                    GSUITE_DOMAIN_NAME = "scienceandpizza.com"
                    if idinfo['hd'] != GSUITE_DOMAIN_NAME:
                        raise ValueError('Wrong hosted domain.')

                    # ID token is valid. Get the user's Google Account ID from the decoded token.
                    userid = idinfo['sub']
                    print(idinfo["email"] + " Authentication Complete")
                    self.clients.append(ServerClient(token, "g_user", websocket, idinfo["email"]))
                    websocket.user = {"user_type":"g_user", "user_id":idinfo["email"]}
                except ValueError as e:
                    tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
                    print(tb_str)
                    print("Invalid Token")
                    print(cmd["id_token"])
                    # Invalid token
                    pass

            if cmd["cmdType"] == "a_auth":
                a_users = self.dbs["config"].get("a_users", "backend")
                if cmd["id"] in a_users:
                    if cmd["token"] in a_users[cmd["id"]]["tokens"]:
                        websocket.user = {"user_type":"a_user", "user_id":cmd["id"]}
                        response = json.dumps({"cmdType": "a_auth_response", "msg": "success"})
                        await websocket.send(response)
                    else:
                        response = json.dumps({"cmdType": "a_auth_response", "msg": "Failed, token incorrect"})
                        await websocket.send(response)
                else:
                    response = json.dumps({"cmdType": "a_auth_response", "msg": "Failed, a_user not found"})
                    await websocket.send(response)

    def get_response_packet(self, key, db_key, user):
        return json.dumps({"cmdType": "getResp", "key": key, "val": self.dbs[db_key].get(key, user), "db_key": db_key})

    def write_to_disk(self, db_key):
        if db_key != "":
            self.dbs[db_key].write_to_disk()
        else:
            for db in self.dbs:
                self.dbs[db].write_to_disk()
    
    def read_from_disk(self, db_key):
        self.dbs[db_key] = Database(db_key, read=True, root_dir=self.rootDir)

    def load_config(self):
        try:
            print("Loading ACI Server version " + ACIVersion)
            self.read_from_disk("config")
            self.port = self.dbs["config"].get("port", "backend")
            self.ip = self.dbs["config"].get("ip", "backend")
            self.rootDir = self.dbs["config"].get("rootDir", "backend")
            for db in self.dbs["config"].get("dbs", "backend"):
                self.read_from_disk(db)
            print("Config read complete")
        except Exception:
            traceback.print_exc()
            print("Unable to read config. Please initialize databases manually.")

