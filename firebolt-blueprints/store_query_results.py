import client
import argparse
import os
import sys


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
    parser.add_argument('--engine-name', dest='engine', required=True)
    parser.add_argument('--query', dest='query', required=True)
    parser.add_argument(
            '--start-wait-engine',
            dest='start_wait_engine',
            default='True',
            required=False,
            )
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default='output.csv',
        required=True)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--file-header',
        dest='file_header',
        default='True',
        required=False)
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


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')

    return combined_name


def write_result(result, full_path, file_header):
    column_names = parse_column_names(result)

    with open(full_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        if file_header:
            writer.writerow(column_names)

        for row in result['data']:
            writer.writerow(create_data_row(column_names, row))


def parse_column_names(result):
    names = []
    for column in result['meta']:
        names.append(column['name'])
    return names


def create_data_row(column_names, row):
    result = []
    for column_name in column_names:
        result.append(row[column_name])
    return result


def main():
    args = get_args()
    email = args.email
    password = args.password
    database = args.database
    engine_name = args.engine_name
    query = args.query
    start_wait_engine = convert_to_boolean(args.start_wait_engine)
    destination_file_name = args.destination_file_name
    destination_folder_name = args.destination_folder_name
    destination_full_path = combine_folder_and_file_name(
        folder_name=destination_folder_name, file_name=destination_file_name)
    file_header = convert_to_boolean(args.file_header)

    if not os.path.exists(destination_folder_name) and (
            destination_folder_name != ''):
        os.makedirs(destination_folder_name)

    try:
        client = client.Client(email, password)

        engine_id = client.get_engine_id(engine_name)
        engine = client.describe_engine(engine_id)
        endpoint = engine['endpoint']

        if start_wait_engine:
            client.start_engine(engine_id)
            client.wait_engine_status(engine_id, client.ENGINE_STATUS_RUNNING)

        write_result(result, destination_full_path, file_header)

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

