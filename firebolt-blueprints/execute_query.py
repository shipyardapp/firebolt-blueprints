import client
import argparse
import sys


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--email', dest='email', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--database', dest='database', required=True)
    parser.add_argument('--engine-name', dest='engine', required=True)
    parser.add_argument('--query', dest='query', required=True)
    parser.add_argument('--start-wait-engine', dest='start_wait_engine', default='True', required=False)
    args = parser.parse_args()

    return args


def convert_to_boolean(string):
    """
    Shipyard can't support passing Booleans to code, so we have to convert
    string values to their boolean values.
    """
    if string in ['True', 'true', 'TRUE']:
        value = True
    else:
        value = False
    return value


def main():
    args = get_args()
    email = args.email
    password = args.password
    database = args.database
    engine_name = args.engine_name
    query = args.query
    start_wait_engine = convert_to_boolean(args.start_wait_engine)

    try:
        client = client.Client(email, password)

        engine_id = client.get_engine_id(engine_name)
        engine = client.describe_engine(engine_id)
        endpoint = engine['endpoint']

        if start_wait_engine:
            client.start_engine(engine_id)
            client.wait_engine_status(engine_id, client.ENGINE_STATUS_RUNNING)

        result = client.execute(endpoint, database, query)
        print(result.text)

    except Exception as e:
        print(e)

        exit_code = EXIT_CODE_UNKNOWN_ERROR

        if isinstance(e, client.AuthenticationError):
            exit_code = EXIT_CODE_AUTHENTICATION_ERROR
        elif isinstance(e, client.EngineWrongStatusError):
            exit_code = EXIT_CODE_ENGINE_WRONG_STATUS
        elif isinstance(e, client.RequestError):
            if e.is_client_error():
                exit_code = EXIT_CODE_REQUEST_CLIENT_ERROR
            else:
                exit_code = EXIT_CODE_REQUEST_SERVER_ERROR
        
        sys.exit(exit_code)


if __name__ == '__main__':
    main()
