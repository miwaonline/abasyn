import threading
import queue
import time
import fdb
from sysutils import logger, config

local_db_config = config["local_db"]
remote_db_query = config["remote_db_query"]

event_queue = queue.Queue()
total_processed = 0
remote_db_status = {
    "connected": False,
    "since": None,
    "last_successful_connect": None,
}
processing_thread = None
stop_event = threading.Event()


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
    logger.info(f"Stopped trying to connect to {dsn} due to stop event")


def get_remote_db_config():
    con = connect_to_database(**local_db_config)
    if con is None:
        return None
    cur = con.cursor()
    cur.execute(remote_db_query)
    result = cur.fetchone()
    con.close()
    return {"dsn": result[0], "user": result[1], "password": result[2]}


def listen_for_events(event_list):
    con = connect_to_database(**local_db_config)
    if con is None:
        return
    logger.info("Started listening for replicate events")
    try:
        while not stop_event.is_set():
            with con.event_conduit(event_list) as event_cond:
                events = event_cond.wait(10)
                if (
                    (events is not None)
                    and len(events)
                    and events["replication"] > 0
                ):
                    logger.info(f"Received event: {events}")
                    event_queue.put(events)
                    if (
                        processing_thread is None
                        or not processing_thread.is_alive()
                    ):
                        start_processing()
    except fdb.fbcore.Error as e:
        logger.error(f"Failed to listen for events, DB error: {e}")
    except Exception as e:
        logger.error(f"Failed to listen for events, error: {e}")
    finally:
        logger.info("Stopped listening for replicate events")


def process_events():
    global total_processed
    remote_db_config = get_remote_db_config()
    if remote_db_config is None:
        return
    remote_con = connect_to_database(**remote_db_config)
    if remote_con is None:
        return
    try:
        while not stop_event.is_set() and not event_queue.empty():
            event = event_queue.get()
            logger.info(f"Processing event: {event}")
            remote_db_config = get_remote_db_config()
            if remote_db_config is None:
                continue
            remote_con = connect_to_database(**remote_db_config)
            if remote_con is None:
                continue
            cur = remote_con.cursor()
            cur.execute("INSERT INTO repl_log (timestamp) VALUES (1);")
            remote_con.commit()
            remote_con.close()
            total_processed += 1
            logger.info(f"Event processed: {event}")
    except fdb.fbcore.DatabaseError as e:
        logger.error(f"Failed to process event, DB error: {e}")
    except Exception as e:
        logger.error(f"Failed to process event, error: {e}")
    finally:
        logger.info("Stopped processing events")
        remote_con.close()


def start_listening():
    listener_thread = threading.Thread(
        target=listen_for_events, args=(["replication"],)
    )
    listener_thread.start()
    return listener_thread


def start_processing():
    processing_thread = threading.Thread(target=process_events)
    processing_thread.start()
    return processing_thread


def stop_event_processing():
    stop_event.set()
