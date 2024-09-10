from flask import Blueprint, jsonify, request
from sysutils import logger, version
import db
api = Blueprint("api", __name__)


@api.route("/api/status/", methods=["GET"])
def status():
    logger.info("Getting app status")
    listener = db.listener_thread()
    res = listener.status() if listener else {"status": "stopped"}
    res['version'] = version
    return jsonify(res)


@api.route("/api/repl/check/<id>/", methods=["GET"])
def check(id):
    logger.info("Checking a commodity history differences")
    result = db.check_tovar_history(id)
    return jsonify(result)


@api.route("/api/repl/check_all/", methods=["GET"])
def check_all():
    logger.info("Checking all commodities history differences")
    result = {"status": "to be implemented"}
    return jsonify(result)


@api.route("/api/repl/status/", methods=["GET"])
def sync_status():
    logger.info("Checking replication status")
    result = db.check_replication_status()
    return jsonify(result)


@api.route("/api/repl/initialize/", methods=["POST"])
def sync_initialize():
    logger.info("Initializing replication")
    result = db.init_replication()
    return jsonify(result)


@api.route("/api/repl/receiver/", methods=["POST"])
@api.route("/api/repl/receiver/<id>/", methods=["GET", "DELETE"])
def receiver(id=None):
    if request.method == "GET":
        logger.info("Getting receiver info")
        result = db.get_receiver(id)
    elif request.method == "POST":
        logger.info("Creating receiver")
        alias = request.get_json()["alias"]
        dbname = request.get_json()["dbname"]
        dbuser = request.get_json().get("dbuser", "SYSDBA")
        dbpass = request.get_json().get("dbpass", "masterkey")
        result = db.add_receiver(alias, dbname, dbuser, dbpass)
    elif request.method == "DELETE":
        logger.info("Deleting receiver")
        result = db.del_receiver(id)
    return jsonify(result)


@api.route("/api/repl/receivers/", methods=["GET"])
def receivers():
    logger.info("Listing receivers")
    result = db.get_receivers()
    return jsonify(result)
