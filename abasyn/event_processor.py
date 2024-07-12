import threading
import queue
import time
from db import connect_to_database, get_remote_db_config, set_stop_event
from sysutils import logger, config
import fdb


local_db_config = config["local_db"]
event_queue = queue.Queue()
total_processed = 0
remote_db_status = {
    "connected": False,
    "since": None,
    "last_successful_connect": None,
}

stop_event = threading.Event()
set_stop_event(stop_event)  # Pass the stop event to db module


def listen_for_events(event_list):
    con = connect_to_database(**local_db_config)
    if con is None:
        return
    logger.info("Started listening for replicate events")
    try:
        while not stop_event.is_set():
            with con.event_conduit(event_list) as event_cond:
                # Without timeout wait() forever fails to stop; 10 seconds is
                # a reasonable compromise
                events = event_cond.wait(10)
                if (
                    (events is not None)
                    and len(events)
                    and events["replication"] > 0
                ):
                    logger.info(f"Received event: {events}")
                    event_queue.put(events)
    except fdb.fbcore.Error as e:
        logger.error(f"Failed to listen for events, DB error: {e}")
    except Exception as e:
        logger.error(f"Failed to listen for events, error: {e}")
    finally:
        logger.info("Stopped listening for replicate events")


def process_events():
    global total_processed
    while not event_queue.empty() or not stop_event.is_set():
        if not event_queue.empty():
            event = event_queue.get()
            logger.info(f"Processing event: {event}")
            remote_db_config = get_remote_db_config()
            if remote_db_config is None:
                continue
            remote_con = connect_to_database(**remote_db_config)
            if remote_con is None:
                continue
            cur = remote_con.cursor()
            cur.execute("INSERT INTO s_messages (text) VALUES ('Event');")
            remote_con.commit()
            remote_con.close()
            total_processed += 1
            logger.info(f"Event processed: {event}")
        else:
            time.sleep(1)
    logger.info("All events processed, exiting thread")


def start_listening():
    listener_thread = threading.Thread(
        target=listen_for_events(["replication"])
    )
    listener_thread.start()
    listener_thread.join()
    logger.info("Stopped listening for events")


def start_processing():
    while not stop_event.is_set():
        if not event_queue.empty():
            logger.info("Processing events in the queue")
            processing_thread = threading.Thread(target=process_events)
            processing_thread.start()
            processing_thread.join()
        else:
            time.sleep(1)


def stop_event_processing():
    stop_event.set()
