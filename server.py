from secret_manager import access_secret
import json
from flask import Flask, request,jsonify
from db2 import get_records, get_authorized_users, get_data_from_bq, get_last_sync
from gpt import get_analytics
from flask_cors import CORS
from flask_sslify import SSLify
from flask_talisman import Talisman


app = Flask(__name__)
# Talisman(app)
# cors = CORS(app)


def require_api_key(view_function):
    from functools import wraps
    @wraps(view_function)
    def decorated_function(*args, **kwargs):
        headers_api_key = request.headers.get("X-API-KEY")
        if headers_api_key != access_secret("kitrum-cloud", "vm_api_key"):
            return jsonify({"error": "Unauthorized"}), 401
        return view_function(*args, **kwargs)
    return decorated_function


@app.route('/authed_users', methods=['GET'])
@require_api_key
def get_users():
    if request.method == 'GET':
        result = get_authorized_users()
        return jsonify(result)


@app.route('/crm-email-tool/fetch_email', methods=['GET'])
@require_api_key
def fetch_mail():
    if request.method == 'GET':
        request_data = request.args
        email_address = request_data.get("email")
        result = get_records(email_address.lower())
        return jsonify({"result": result})


@app.route('/crm-email-tool/get_analytics', methods=['POST'])
@require_api_key
def gpt_handler():
    if request.method == 'POST':
        request_data = request.data
        result = get_analytics(request_data.decode("utf-8"))
        print(result)
        return jsonify(result)


@app.route('/crm-email-tool/get_sync_details', methods=['GET'])
@require_api_key
def get_sync_details():
    if request.method == 'GET':
        response = get_last_sync()
    return jsonify(response)


app.run(host='0.0.0.0', port=7233)

