import threading
import queue
import time
import datetime
import fdb
from sysutils import logger, config
import platform

event_queue = queue.Queue()
stop_event = threading.Event()
lock = threading.Lock()
_listener_thread = None


class ProcessingThread(threading.Thread):
    def __init__(self, local_connect, remote_dbconf):
        super().__init__()
        self.local_connect = local_connect
        self.remote_dbconf = remote_dbconf
        self.remote_connect = None
        self.records_processed = 0

    def run(self):
        dsn, user, password, last_id = self.remote_dbconf
        self.remote_connect = connect_to_database(dsn, user, password)
        if self.remote_connect is None:
            return
        try:
            while not stop_event.is_set() and not event_queue.empty():
                event = event_queue.get()
                logger.info(f"Processing event: {event}")
                # pull replication data from local db
                localcur = self.local_connect.cursor()
                sql = f"select id, rpl_sql from rpl_log where id > {last_id}"
                localcur.execute(sql)
                changes = localcur.fetchall()
                self.local_connect.rollback()
                # push replication data to remote db
                remotecur = self.remote_connect.cursor()
                q = ("select rdb$set_context('USER_SESSION', "
                     "'replicating_now', 1) from rdb$database")
                remotecur.execute(q)
                for change in changes:
                    logger.debug(f"Pushing change: {change[1]}")
                    remotecur.execute(change[1])
                    self.records_processed += 1
                    last_pushed_id = change[0]
                self.remote_connect.commit()
                # update last_id
                sql = f"update rpl_databases set last_id = {last_pushed_id}"
                localcur.execute(sql)
                self.local_connect.commit()
                logger.info(f"Pushed {self.records_processed} records")
        except fdb.fbcore.DatabaseError as e:
            logger.error(f"Failed to process event, DB error: {e}")
        except Exception as e:
            logger.error(f"Failed to process event, error: {e}")
        finally:
            logger.info("Stopped processing events")
            self.remote_connect.close()


class ListeningThread(threading.Thread):
    def __init__(self, event_list):
        super().__init__()
        self.masterdb = config["database"]
        self.local_conn = None
        self.event_list = event_list
        self.processing_thread = None
        self.events_processed = 0
        self.records_processed = 0
        self.remote_connect_time = None

    def start_processing(self):
        cur = self.local_conn.cursor()
        sql = "SELECT dbname, dbuser, dbpass, last_id FROM rpl_databases"
        cur.execute(sql)
        result = cur.fetchone()
        self.local_conn.rollback()
        self.processing_thread = ProcessingThread(self.local_conn, result)
        self.events_processed += 1
        self.processing_thread.start()
        timenow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.remote_connect_time = timenow
        self.processing_thread.join()

    def status(self):
        if self.processing_thread and self.processing_thread.is_alive():
            ownstatus = "processing"
            remote_db_status = {
                "connected": True,
                "since": self.remote_connect_time,
            }
        else:
            ownstatus = "listening"
            remote_db_status = {
                "connected": False,
                "last_connect": self.remote_connect_time,
            }
        return {
            "status": ownstatus,
            "events_processed": self.events_processed,
            "records_processed": self.records_processed,
            "remote_db_status": remote_db_status,
            "queue_size": event_queue.qsize(),
        }

    # TODO: Currently there is an issue related to the implementation of the
    # database events in FDB, which I dont know how to address. In particular,
    # methods of event conduit raise exceptions which cannot be caught. More
    # than that, they block the shutdown up to the timeout of event conduit.
    # If these execeptions are disabled in the fbcore module, class EventBlock,
    # methods `__wait_for_events` and `close` (lines 2067 and 2092), everything
    # works as expected. Google for "fdb errror while waiting for events" and
    # "fdb error while cancelling events" for details.

    def run(self):
        self.local_conn = connect_to_database(**self.masterdb)
        if self.local_conn is None:
            return
        logger.info("Started listening for replicate events")
        if platform.system() == "Windows":
            timeout = 3
        else:
            timeout = None
        try:
            while not stop_event.is_set():
                with self.local_conn.event_conduit(
                    self.event_list
                ) as event_cond:
                    events = event_cond.wait(timeout)
                    if (
                        (events is not None)
                        and len(events)
                        and (events.get(self.event_list[0], 0) > 0)
                    ):
                        logger.info(f"Received event: {events}")
                        event_queue.put(events)
                        if (
                            self.processing_thread is None
                            or not self.processing_thread.is_alive()
                        ):
                            self.start_processing()
        except fdb.fbcore.Error as e:
            logger.error(f"Failed to listen for events, DB error: {e}")
        except Exception as e:
            logger.error(f"Failed to listen for events, error: {e}")
        finally:
            logger.info("Stopped listening for replicate events")


def connect_to_database(dsn, user, password, timeout=60):
    global stop_event
    start_time = time.time()
    while not stop_event.is_set():
        try:
            if time.time() - start_time > timeout:
                logger.info(
                    f"Timeout of {timeout} seconds reached while "
                    f"trying to connect to {dsn}"
                )
                return None
            logger.info(f"Trying to connect to {dsn}")
            con = fdb.connect(dsn=dsn, user=user, password=password)
            logger.info(f"Successfully connected to {dsn}")
            return con
        except fdb.fbcore.DatabaseError as e:
            logger.error(f"Failed to connect to {dsn}, error: {e}")
            time.sleep(10)
    logger.info(f"Stopped trying to connect to {dsn} due to stop event")
    return None


def listener_thread():
    global _listener_thread
    if _listener_thread is None:
        _listener_thread = ListeningThread(["replicate"])
        _listener_thread.start()
    return _listener_thread


def stop_event_processing():
    stop_event.set()


def levenshtein_distance_operations(list1, list2):
    len_list1 = len(list1) + 1
    len_list2 = len(list2) + 1
    # Create a matrix to store distances and operations
    matrix = [[0 for n in range(len_list2)] for m in range(len_list1)]
    operations = [[[] for n in range(len_list2)] for m in range(len_list1)]
    # Initialize the first row and column of the matrix
    for i in range(len_list1):
        matrix[i][0] = i
        if i > 0:
            operations[i][0] = operations[i - 1][0] + [
                ("delete", list1[i - 1], i - 1)
            ]
    for j in range(len_list2):
        matrix[0][j] = j
        if j > 0:
            operations[0][j] = operations[0][j - 1] + [
                ("insert", list2[j - 1], j - 1)
            ]
    # Compute the Levenshtein distance and operations
    for i in range(1, len_list1):
        for j in range(1, len_list2):
            if list1[i - 1] == list2[j - 1]:
                cost = 0
                operation = []
            else:
                cost = 1
                operation = [
                    ("substitute", list1[i - 1], list2[j - 1], i - 1, j - 1)
                ]

            deletion_cost = matrix[i - 1][j] + 1
            insertion_cost = matrix[i][j - 1] + 1
            substitution_cost = matrix[i - 1][j - 1] + cost

            min_cost = min(deletion_cost, insertion_cost, substitution_cost)
            matrix[i][j] = min_cost

            if min_cost == deletion_cost:
                operations[i][j] = operations[i - 1][j] + [
                    ("delete", list1[i - 1], i - 1)
                ]
            elif min_cost == insertion_cost:
                operations[i][j] = operations[i][j - 1] + [
                    ("insert", list2[j - 1], j - 1)
                ]
            else:
                operations[i][j] = operations[i - 1][j - 1] + operation

    return matrix[-1][-1], operations[-1][-1]


def check_tovar_id(tovar_id):
    local = connect_to_database(**config["local_db"])
    if local is None:
        return {"status": "local db not available"}
    cur = local.cursor()
    cur.execute("SELECT dbname, dbuser, dbpass FROM rpl_databases")
    dsn, user, password = cur.fetchone()
    sql = (
        "select tovar_id, tovar_code, cast(date_op as char(10)), amount,"
        "pos_id, doc_id, doc_type, snd_storage, rcv_storage "
        "from m_tovar where tovar_id = ?"
        "order by date_op, id"
    )
    cur.execute(sql, (tovar_id))
    local_result = cur.fetchall()
    local.close()
    remote = connect_to_database(dsn, user, password)
    if remote is None:
        return {"status": "remote db not available"}
    cur = remote.cursor()
    cur.execute(sql)
    remote_result = cur.fetchall()
    remote.close()
    distance, operations = levenshtein_distance_operations(
        local_result, remote_result
    )
    return {
        "status": "ok",
        "local_length": len(local_result),
        "remote_length": len(remote_result),
        "distance": distance,
        "operations": operations,
    }
