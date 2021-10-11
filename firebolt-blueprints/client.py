import os
import requests

ENV_FIREBOLT_EMAIL = 'FIREBOLT_EMAIL'
ENV_FIREBOLT_PASSWORD = 'FIREBOLT_PASSWORD'

FIREBOLT_LOGIN_URL = 'https://api.app.firebolt.io/auth/v1/login'

class AuthenticatedClient:
    def __init__(self, email=None, password=None):
        if email is None:
            email = os.environ.get(ENV_FIREBOLT_EMAIL)
        if password is None:
            password = os.environ.get(ENV_FIREBOLT_PASSWORD)

        try:
            self.token = _get_authorization_token(email, password)
        except:
            raise AuthenticationError(email)

        print(self.token)

    def get_engine_name(self, engine_name):
        return None

    def start_engine(self, engine_id):
        return None

    def stop_engine(self, engine_id):
        return None

    def restart_engine(self, engine_id):
        return None

    def get_engine_url(self, engine_name):
        return None

def _get_authorization_token(email, password):
    resp = requests.post(FIREBOLT_LOGIN_URL, json={
        'username': email,
        'password': password,
        })
    return resp.json()['access_token']

class AuthenticationError(Exception):
    def __init__(self, email):
        self.email = email
