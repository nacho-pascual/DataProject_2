import os
import logging
import pytz
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)


gunicorn_logger = logging.getLogger('gunicorn.info')
app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(gunicorn_logger.level)


def franja_horaria(str_timestamp):
    dt = datetime.strptime(str_timestamp, '%Y-%m-%d %H:%M:%S.%f%z')
    # Convertir de UTC a tiempo local
    dt = dt.replace(tzinfo=pytz.timezone('Europe/Madrid')).astimezone()
    hour = dt.hour

    # weekday 0 => lunes, 5 => sábado, 6 => domingo
    if (dt.weekday() in [5, 6]) or (hour in [0, 1, 2, 3, 4, 5, 6, 7]):
        return 'valle'

    if hour in [8, 9, 14, 15, 16, 17, 22, 23]:
        return 'llano'

    return 'punta'


@app.route("/status", methods=['GET'])
def get_status():
    return jsonify({"status": "OK"})


@app.route('/franjas', methods=['POST'])
def franjas():
    message = request.get_json()
    # print(message)
    message['franja'] = franja_horaria(message['timestamp'])

    return jsonify(message)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

