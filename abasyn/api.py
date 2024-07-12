from flask import Blueprint, jsonify
from event_processor import event_queue, total_processed, remote_db_status
from db import connect_to_database, local_db_config
from sysutils import logger

api = Blueprint('api', __name__)


@api.route('/api/status', methods=['GET'])
def status():
    logger.info("Getting status")
    return jsonify({
        'queue_size': event_queue.qsize(),
        'total_processed': total_processed,
        'remote_db_status': remote_db_status
    })


@api.route('/api/check', methods=['GET'])
def check():
    logger.info("Checking")
    con = connect_to_database(**local_db_config)
    cur = con.cursor()
    cur.execute("EXECUTE PROCEDURE service_check;")
    result = cur.fetchone()
    con.close()
    return jsonify(result)
