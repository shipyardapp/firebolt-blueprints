import argparse
import sys

# Handle import difference between local and github install
try:
    import client
except BaseException:
    from . import client

EXIT_CODE_UNKNOWN_ERROR = 3
EXIT_CODE_ENGINE_WRONG_STATUS = 200
EXIT_CODE_AUTHENTICATION_ERROR = 201
EXIT_CODE_REQUEST_CLIENT_ERROR = 202
EXIT_CODE_REQUEST_SERVER_ERROR = 203


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--email', dest='email', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--database', dest='database', required=True)
    parser.add_argument('--engine-name', dest='engine_name', required=True)
    parser.add_argument('--query', dest='query', required=True)
    parser.add_argument(
        '--start-wait-engine',
        dest='start_wait_engine',
        default='True',
        required=False,
    )
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
        fc = client.Client(email, password)

        engine_id = fc.get_engine_id(engine_name)
        engine = fc.describe_engine(engine_id)
        endpoint = engine['endpoint']

        if start_wait_engine:
            fc.start_engine(engine_id)
            fc.wait_engine_status(engine_id, client.ENGINE_STATUS_RUNNING)

        result = fc.execute(endpoint, database, query)
        print(result.text)

    except Exception as e:
        print(e, file=sys.stderr)

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
