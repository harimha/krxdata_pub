
class Configuration():
    def __init__(self):
        self.secrets = self._get_secret()
        self.password = self.secrets["password"]
        self.base_server_path = self.secrets["base_server_path"]
        self.base_db_path =self.secrets["base_db_path"]
        self.mysqldump = self.base_server_path + "mysqldump"
        self.mysql = self.base_server_path + "mysql"
        self.user = "root"
        self.host = "localhost"
        self.port = "3306"

    def _get_secret(self):
        secrets_dict = {}
        with open("db/secret/secrets.txt", "r") as f:
            for line in f.readlines():
                key = line.strip().split("=")[0].strip()
                val = line.strip().split("=")[1].strip()
                secrets_dict[key] = val

        return secrets_dict

