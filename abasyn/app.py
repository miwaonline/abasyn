import signal
import platform
import sys
from flask import Flask
from waitress import serve
from api import api
from sysutils import logger, config
from db import (
    listener_thread,
    stop_event_processing,
)

app = Flask(__name__)
app.register_blueprint(api)


def signal_handler(sig, frame):
    logger.info("Received signal: %s", sig)
    shutdown()


def shutdown():
    try:
        stop_event_processing()
        logger.info("Stopping the service gracefully")
        if listener.is_alive():
            logger.info("Joining the listener thread")
            listener.join()
            logger.info("Listener thread joined")
        sys.exit(0)
    except Exception as e:
        logger.error("Exiting failed: %s", e)
        sys.exit(1)


def setup_signal_handlers():
    signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C
    if platform.system() == "Windows":
        signal.signal(signal.SIGBREAK, signal_handler)  # Handle Ctrl+Break
    signal.signal(signal.SIGTERM, signal_handler)  # Handle termination signals


if __name__ == "__main__":
    setup_signal_handlers()
    listener = listener_thread()
    try:
        serve(app, host="0.0.0.0", port=config["webservice"]["port"])
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
        shutdown()
    except Exception as e:
        logger.error("Error running the app: %s", e)
    finally:
        if listener.is_alive():
            listener.join()
