import signal
import sys
from flask import Flask
from api import api
from sysutils import logger
from db import (
    start_listening,
    stop_event_processing,
)

app = Flask(__name__)
app.register_blueprint(api)


def signal_handler(sig, frame):
    logger.info("Gracefully shutting down...")
    stop_event_processing()
    logger.info("Shutdown complete.")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)
    listener_thread = start_listening()
    app.run(debug=False)
    listener_thread.join()


if __name__ == "__main__":
    main()
