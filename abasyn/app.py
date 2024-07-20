import signal
import sys
from flask import Flask
from api import api
from sysutils import logger, config
from db import (
    listener_thread,
    stop_event_processing,
)

app = Flask(__name__)
app.register_blueprint(api)


def signal_handler(sig, frame):
    try:
        stop_event_processing()
        sys.exit(0)
    except Exception as e:
        logger.info("Exiting failed: %s", e)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)
    listener = listener_thread()
    app.run(debug=False, host="0.0.0.0", port=config["webservice"]["port"])
    try:
        logger.info("Joining the listener thread")
        listener.join()
        logger.info("Listener thread joined")
    except Exception as e:
        logger.info("Listener thread join failed: %s", e)


if __name__ == "__main__":
    main()
