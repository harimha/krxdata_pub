import os
import subprocess
from datetime import datetime
from krxdata.db.configure import Configuration


def export_db(db_name, file_name=None):
    if file_name is None:
        file_name = datetime.now().strftime("%Y%m%d%H%M%S")
    c = Configuration()
    cmd = [c.mysqldump,
           f"--password={c.password}",
           f"--user={c.user}",
           f"--host={c.host}",
           f"--port={c.port}",
           f"{db_name}"]
    output_file = c.base_db_path+file_name+".sql"
    with open(output_file, "w") as f:
        subprocess.run(cmd, stdout=f)


def import_db(db_name, file_name=None):
    c = Configuration()
    if file_name is None:
        file_name = os.listdir(c.base_db_path)[-1].replace(".sql","")
    cmd = [c.mysql,
           f"--password={c.password}",
           f"--user={c.user}",
           f"--host={c.host}",
           f"--port={c.port}",
           f"{db_name}"]
    input_file = c.base_db_path+file_name+".sql"
    with open(input_file, "r") as f:
        subprocess.run(cmd, stdin=f)
