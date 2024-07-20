from flask import Blueprint, jsonify
from db import (
    listener_thread,
    check_tovar_id
)
from sysutils import logger

api = Blueprint("api", __name__)


@api.route("/api/status", methods=["GET"])
def status():
    logger.info("Getting status")
    listener = listener_thread()
    return jsonify(
        listener.status() if listener else {"status": "stopped"}
        )


@api.route("/api/check/<tovar_id>/", methods=["GET"])
def check(tovar_id):
    logger.info("Checking")
    result = check_tovar_id(tovar_id)
    return jsonify(result)


@api.route("/api/check_all", methods=["GET"])
def check_all():
    logger.info("Checking all")
    result = {"status": "to be implemented"}
    return jsonify(result)


@api.route("/api/sync/status/", methods=["GET"])
def sync_status():
    logger.info("Sync status")
    result = {"status": "to be implemented"}
    return jsonify(result)


@api.route("/api/sync/initialize", methods=["POST"])
def initialize_sync():
    logger.info("Initializing")
    result = {"status": "to be implemented"}
    return jsonify(result)


@api.route("/api/sync/receiver/", methods=["GET", "POST"])
def sync_receiver():
    logger.info("Syncing")
    result = {"status": "to be implemented"}
    return jsonify(result)
