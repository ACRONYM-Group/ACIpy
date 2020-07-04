import json
import os
import traceback


ACIVersion = "2020.07.01.1"

class Item:
    def __init__(self, key, value, owner, read=False, root_dir="./", read_db="", type="string"):
        self.key = key
        self.value = value
        self.owner = owner
        self.subs = []
        self.root_dir = root_dir
        self.permissions = {}
        self.type = type
        self.ver = ACIVersion
        self.maxLen = 100000

        if read:
            self.read_from_disk(read_db)
    
    def get_val(self, user):
        hasPermission = self.authenticate(user, "read")

        if hasPermission == True:
            return self.value
        else:
            return "Access Denied: Your User ID is not listed in the item permissions table."

    def set_val(self, value, user):
        hasPermission = self.authenticate(user, "write")

        if hasPermission:
            self.value = value
            return self.value
        else:
            return "Access Denied: Your User ID is not listed in the item permissions table."

    def get_index(self, index, user):
        if self.authenticate(user, "read"):
            try:
                index = json.loads(index)
            except:
                pass
            if not isinstance(index, list):
                indexs = [index]
            else:
                indexs = index

            table = self.get_val(user)
            if isinstance(table, list):
                values = {}
                for x in range(len(indexs)):
                    values[int(indexs[x])] = table[int(indexs[x])]
                    
                return values
            else:
                return "ERROR - " + table
        else:
            return "Access Denied"
             
    
    def set_index(self, index, value, user):
        if self.authenticate(user, "write"):
            try:
                index = json.loads(index)
                value = json.loads(value)
            except Exception as e:
                tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
                print(tb_str)
                return "ERROR - Failed to read command ind(ex/ices) and value(s)"
            if not isinstance(index, list):
                indexs = [index]
                values = {int(index):value}
            else:
                indexs = index
                values = value

            table = self.get_val(user)
            if isinstance(table, list):
                for x in range(len(indexs)):
                    if (int(indexs[x]) < len(table)):
                        table[int(indexs[x])] = values[str(indexs[x])]
                    else:
                        return "ERROR - index does not exist"

                self.set_val(table, user)
                return "Success"
            else:
                return "ERROR"
        else:
            return "Access Denied"

    def append_index(self, value, user):
        if self.authenticate(user, "write"):
            if not isinstance(value, list):
                values = [value]
            else:
                values = value

            table = self.get_val(user)
            if isinstance(table, list):
                for x in range(len(values)):
                    table.append(values[x])
                
                if len(table) > self.maxLen:
                    while len(table) > self.maxLen:
                        table.pop(0)

                self.set_val(table, user)
                return "Success"
            else:
                return "ERROR"
        else:
            return "Access Denied"

    def get_len(self, user):
        if self.authenticate(user, "read"):
            table = self.get_val(user)
            return len(table)

    def get_recent(self, num, user):
        num = int(num)
        if self.authenticate(user, "read"):
            table = self.get_val(user)
            values = []
            i = 0
            while i < num:
                values.append(table[len(table)-num+i])
                i += 1
            return values
    
    def authenticate(self, user, permission):
        hasPermission = False
        if user == "backend":
            hasPermission = True
        elif permission in self.permissions:
            for userPermission in self.permissions["write"]:
                if user == "NotAuthed":
                    if userPermission[0] == "a_user" and userPermission[1] == "any":
                        hasPermission = True
                else:
                    if userPermission[0] == user["user_type"] and userPermission[1] == user["user_id"] or userPermission[1] == "authed":
                        hasPermission = True
                    if userPermission[0] == user["user_type"] and userPermission[1] == "any" or userPermission[1] == "authed":
                        hasPermission = True

        if hasPermission:
            return True
        else:
            return False


    def upgrade_item(self, data):

        #Is it a Legacy list based item?
        if isinstance(data, list):
            print("Upgrading legacy list item to current")
            new_data = {
                "key" : data[0],
                "value" : data[1],
                "owner" : data[2],
                "permissions" : data[3],
                "subs" : data[4],
                "type": "string",
                "ver" : self.ver
            }

            return new_data


    def write_to_disk(self, database):
        filename = "./databases/%s/" % database
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != os.errno.EEXIST:
                    raise

        with open("./databases/%s/%s.item" % (database, self.key), 'w') as file:
            file.write(json.dumps({"key":self.key, "value":self.value, "owner":self.owner, "permissions":self.permissions, "subs":self.subs, "type":self.type}))

    def read_from_disk(self, read_db):
        try:
            filename = self.root_dir + "databases/%s/%s.item" % (read_db, self.key)
            with open(filename, 'r') as file:
                print("Reading", filename)
                data = json.loads(file.read())

            if isinstance(data, list):
                data = self.upgrade_item(data)
                    

            self.value = data["value"]
            if self.value == None:
                self.value = ""
            self.owner = data["owner"]
            if self.owner == None:
                self.owner = ""
            self.permissions = data["permissions"]
            if self.permissions == None:
                self.permissions = {"read":[], "write":[]}
            self.subs = data["subs"]
            if self.subs == None:
                self.subs = []
            self.type = data["type"]
            if self.type == None:
                self.type = "string"

        except Exception as e:
            print("WARNING")
            
            print("-Unable to read " + str(self.root_dir + "/databases/" + read_db + "/" + self.key + ".item"))

            tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
            print(tb_str)
            print(" ")


class Database:
    def __init__(self, name, read=False, root_dir="./"):
        self.data = {}
        self.name = name
        self.root_dir = root_dir
        self.ver = ACIVersion

        if read:
            self.read_from_disk()

    def get(self, key, user):
        if key in self.data:
            return self.data[key].get_val(user)
        else:
            return None

    def set(self, key, value, user):
        if not (key in self.data):
            self.new_item(key, value)
        response = self.data[key].set_val(value, user)

        self.data[key].write_to_disk(self.name)

        return response

    def new_item(self, key, value, owner="self"):
        self.data[key] = Item(key, value, owner, root_dir=self.root_dir, permissions={"read":[],"write":[]})

    def upgrade_database(self, data):
        if isinstance(data, list):
            print("Upgrading legacy list database to current")
            new_data = {
                "dbKey": data[0],
                "keys": data[1],
                "ver": self.ver
            }

            return new_data

    def write_to_disk(self):
        filename = "./databases/" + self.name + "/"
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != os.errno.EEXIST:
                    raise

        item_keys = []
        for val in self.data.values():
            val.write_to_disk(self.name)
            item_keys.append(val.key)

        with open(self.root_dir + "databases/%s/%s.database" % (self.name, self.name), "w") as file:
            file.write(json.dumps({"dbKey":self.name, "keys":item_keys, "ver":self.ver}))

    def read_from_disk(self):
        filename = self.root_dir + "databases/%s/%s.database" % (self.name, self.name)
        with open(filename, 'r') as file:
            print(filename)
            db_data = json.loads(file.read())

        if isinstance(db_data, list):
            db_data = self.upgrade_database(db_data)

        for itemKey in db_data["keys"]:
            self.data[itemKey] = Item(itemKey, "None", "None", read=True, read_db=self.name)
