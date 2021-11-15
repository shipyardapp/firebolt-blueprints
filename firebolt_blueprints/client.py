import os
import requests
import time

import pprint

ENV_FIREBOLT_EMAIL = 'FIREBOLT_EMAIL'
ENV_FIREBOLT_PASSWORD = 'FIREBOLT_PASSWORD'


ENGINE_STATUS_STARTING = 'ENGINE_STATUS_RUNNING_REVISION_STARTING'
ENGINE_STATUS_RUNNING = 'ENGINE_STATUS_RUNNING_REVISION_SERVING'
ENGINE_STATUS_IDLE = 'ENGINE_STATUS_RUNNING_IDLE'


FIREBOLT_API_URL = 'https://api.app.firebolt.io'


def _login_url():
    return FIREBOLT_API_URL + '/auth/v1/login'


def _core_url(extra):
    return FIREBOLT_API_URL + '/core/v1' + extra


class Client:
    def __init__(self, email=None, password=None):
        if email is None:
            email = os.environ.get(ENV_FIREBOLT_EMAIL)
        if password is None:
            password = os.environ.get(ENV_FIREBOLT_PASSWORD)

        try:
            self._token = _get_authorization_token(email, password)
        except:
            raise AuthenticationError(email)

        self._request_session = requests.Session()
        self._request_session.headers.update({
            'Authorization': f'Bearer {self._token}',
            })


    def get_engine_id(self, engine_name):
        req = self._new_request(
                'GET',
                _core_url('/account/engines:getIdByName'),
                params={'engine_name': engine_name},
                )

        result = self._do_request(req).json()
        return result['engine_id']['engine_id']


    def wait_engine_status(self, engine_id, status):
        attempt = 0

        engine = self.describe_engine(engine_id)
        attempt += 1

        while engine['current_status'] != status:
            if attempt >= 30:
                break

            time.sleep(60)

            engine = self.describe_engine(engine_id)
            attempt += 1

        if attempt >= 30:
            raise EngineWrongStatusError(engine, status)

        return engine


    def describe_engine(self, engine_id):
        req = self._new_request(
                'GET',
                _core_url(f'/account/engines/{engine_id}'),
                )
        
        return self._do_request(req).json()['engine']


    def start_engine(self, engine_id):
        req = self._new_request(
                'POST',
                _core_url(f'/account/engines/{engine_id}:start'),
                )

        return self._do_request(req).json()['engine']


    def stop_engine(self, engine_id):
        req = self._new_request(
                'POST',
                _core_url(f'/account/engines/{engine_id}:stop'),
                )

        return self._do_request(req).json()['engine']


    def restart_engine(self, engine_id):
        req = self._new_request(
                'POST',
                _core_url(f'/account/engines/{engine_id}:restart'),
                )

        return self._do_request(req).json()['engine']


    def execute(self, endpoint, database, query):
        resp = self._execute_query(endpoint, database, query)
        return resp


    def query(self, endpoint, database, query):
        resp = self._execute_query(endpoint, database, query)
        return resp.json()


    def _execute_query(self, endpoint, database, query):
        req = self._new_request(
                'POST',
                f'https://{endpoint}/',
                headers={'Content-Type': 'application/octet-stream'},
                data=query,
                params={'database': database},
                )

        resp = self._do_request(req)

        if resp.status_code >= 400:
            raise RequestError(req, resp)

        return resp


    def _new_request(self, method, url, headers=None, data=None, params=None):
        req = requests.Request(method, url, headers=headers, data=data, params=params)
        prepped = self._request_session.prepare_request(req)
        return prepped


    def _do_request(self, req):
        resp = self._request_session.send(req)
        return resp


def _get_authorization_token(email, password):
    resp = requests.post(_login_url(), json={
        'username': email,
        'password': password,
        })
    return resp.json()['access_token']


class AuthenticationError(Exception):
    def __init__(self, email):
        self.email = email


    def __str__(self):
        return f'authentication error with {email} and password'


class RequestError(Exception):
    def __init__(self, req, resp):
        self._req = req
        self._resp = resp


    def is_client_error(self):
        status_code = self.status_code()
        return status_code >= 400 and status_code < 500


    def is_server_error(self):
        status_code = self.status_code()
        return status_code >= 500 and status_code < 600


    def status_code(self):
        return self._resp.status_code

    
    def __str__(self):
        return ': '.join([str(self._req), str(self._resp), str(self._resp.text)])


class EngineWrongStatusError(Exception):
    def __init__(self, engine, desired_status):
        self.engine = engine
        self.desired_status = desired_status


