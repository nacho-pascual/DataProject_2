import os
import logging
import json

from flask import Flask, request, jsonify, Response

app = Flask(__name__)


gunicorn_logger = logging.getLogger('gunicorn.info')
app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(gunicorn_logger.level)

def time_band(timestamp):
    return 'valle'


@app.route("/status", methods=['GET'])
def get_status():
    app.logger.info("checking health of application")
    return jsonify({"status": "OK"})


@app.route('/enrich_data', methods=['POST'])
def enrich_data():
    message = request.get_json()
    print(message)
    message['franja'] = time_band(message['timestamp'])

    return jsonify(message)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

