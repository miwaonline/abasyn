import fdb
import time
from sysutils import logger, config

local_db_config = config['local_db']
remote_db_query = config['remote_db_query']
stop_event = None  # This will be set externally


def set_stop_event(event):
    global stop_event
    stop_event = event


def connect_to_database(dsn, user, password):
    while not stop_event.is_set():
        try:
            logger.info(f"Trying to connect to {dsn}")
            con = fdb.connect(dsn=dsn, user=user, password=password)
            logger.info(f"Successfully connected to {dsn}")
            return con
        except fdb.fbcore.DatabaseError as e:
            logger.error(f"Failed to connect to {dsn}, error: {e}")
            time.sleep(10)
    logger.info(f"Stopped trying to connect {dsn} due to stop event")


def get_remote_db_config():
    con = connect_to_database(**local_db_config)
    if con is None:
        return None
    cur = con.cursor()
    cur.execute(remote_db_query)
    result = cur.fetchone()
    con.close()
    return {
        'dsn': result[0],
        'user': result[1],
        'password': result[2]
    }
