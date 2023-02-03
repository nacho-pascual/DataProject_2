from flask import Flask, request, jsonify

app = Flask(__name__)


def time_band(timestamp):
    return 'valle'


@app.route('/enrich_data', methods=['POST'])
def enrich_data():
    message = request.get_json()
    print(message)
    message['franja'] = time_band(message['timestamp'])

    return jsonify(message)


app.run()
