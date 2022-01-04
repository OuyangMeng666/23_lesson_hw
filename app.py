import itertools
import os

from flask import Flask, request
from werkzeug.exceptions import BadRequest

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def read_file(filepath):
    for row in open(filepath, 'r'):
        yield row


def perform_command(logs, command, value):
    if command == 'filter':
        return do_filter(logs, value)
    if command == 'map':
        return "\n".join(do_map(logs, value))
    if command == 'unique':
        return do_unique(logs)
    if command == 'sort':
        return do_sort(logs, value)
    if command == 'limit':
        return do_limit(logs, value)


def do_filter(logs, value):
    return filter(lambda logs_line: value in logs_line, logs)


def do_map(logs, value):
    return map(lambda logs_line: logs_line.split(' ')[int(value)], logs)


def do_unique(logs):
    return set(logs)


def do_sort(logs, value):
    is_asc = value == 'asc'
    return sorted(logs, reverse=is_asc)


def do_limit(logs, count):
    return itertools.islice(logs, 0, int(count))


def build_query(file, query):
    result = perform_command(file, query['cmd1'], query['value1'])
    result = perform_command(result, query['cmd2'], query['value2'])
    return result if result else "Something went wrong"


@app.route("/perform_query", methods=('POST', 'GET'))
def perform_query():
    try:
        file_name = request.args["file_name"]
        query = request.args.to_dict()
    except KeyError:
        raise BadRequest(description="Something wierd happened")

    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        return BadRequest(description=f"{file_name} does not exist")

    logs = read_file(file_path)

    result = build_query(logs, query)

    return app.response_class(result, content_type="text/plain")
