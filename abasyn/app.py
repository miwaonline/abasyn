import threading
import sys
from flask import Flask
from api import api
from event_processor import (
    start_listening, start_processing, stop_event_processing
)
from sysutils import logger

app = Flask(__name__)
app.register_blueprint(api)


def signal_handler(sig, frame):
    logger.info('Gracefully shutting down...')
    stop_event_processing()
    sys.exit(0)


def main():
    start_listening()
    threading.Thread(target=start_processing).start()
    app.run(debug=True)


if __name__ == '__main__':
    main()
