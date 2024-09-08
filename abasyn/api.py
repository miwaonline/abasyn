from flask import Blueprint, jsonify, request
from sysutils import logger
import db
api = Blueprint("api", __name__)


@api.route("/api/status", methods=["GET"])
def status():
    logger.info("Getting status")
    listener = db.listener_thread()
    return jsonify(
        listener.status() if listener else {"status": "stopped"}
        )


@api.route("/api/repl/check/<tovar_id>/", methods=["GET"])
def check(tovar_id):
    logger.info("Checking a commodity history differences")
    result = db.check_tovar_id(tovar_id)
    return jsonify(result)


@api.route("/api/repl/check_all", methods=["GET"])
def check_all():
    logger.info("Checking all commodities history differences")
    result = {"status": "to be implemented"}
    return jsonify(result)


@api.route("/api/repl/status/", methods=["GET"])
def sync_status():
    logger.info("Checking replication status")
    result = db.check_replication_status()
    return jsonify(result)


@api.route("/api/repl/initialize", methods=["POST"])
def sync_initialize():
    logger.info("Initializing replication")
    result = {"status": "to be implemented"}
    return jsonify(result)


@api.route("/api/repl/receiver/", methods=["GET", "POST", "DELETE"])
def sync_receiver():
    if request.method == "GET":
        logger.info("Getting receiver info")
    elif request.method == "POST":
        logger.info("Creating receiver")
    elif request.method == "DELETE":
        logger.info("Deleting receiver")
    result = {"status": "to be implemented"}
    return jsonify(result)
