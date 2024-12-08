import itertools
import random
import requests
import string
import time

SERVER_PORTS = [5030, 5031, 5032]

port_gen = itertools.cycle(SERVER_PORTS)

KEY_LEN = 1
VALUE_LEN = 3

REQ_TRIES = 2 * len(SERVER_PORTS)

REQ_NUMBER = 10

TIMEOUT = 3

def gen_string(length):
    s = random.choices(string.ascii_letters, k=length)
    return ''.join(s)

def gen_key_value(operation):
    return {"Key": gen_string(KEY_LEN), "Value": gen_string(VALUE_LEN), "Op": operation}

def gen_key(operation):
    return {"Key": gen_string(KEY_LEN), "Op": operation}

def generate_request_data() -> dict:
    op = random.choice(['create', 'read', 'update', 'delete'])
    if op in ('create', 'update'):
        return gen_key_value(op)
    else:
        return gen_key(op)

def send_data(data, port) -> requests.Response:
    if data['Op'] == 'read':
        return requests.get(url=f'http://localhost:{port}/apply', json=data, timeout=TIMEOUT)
    return requests.post(url=f'http://localhost:{port}/apply', json=data, timeout=TIMEOUT)

check_dict = dict()

def apply_operation(data):
    global check_dict
    op = data["Op"]
    if op == 'read':
        return
    if op in ('create', 'update'):
        check_dict[data['Key']] = data['Value']
        return
    if op == 'delete':
        check_dict.pop(data['Key'], None)

def send_random_request():
    data = generate_request_data()
    apply_operation(data)
    for _ in range(REQ_TRIES):
        time.sleep(1)
        try:
            port = next(port_gen)
            print(port)
            resp = send_data(data, port)
        except:
            print('Timeout')
            continue
        print(resp.status_code)
        if resp.status_code >= 200 and resp.status_code < 300:
            return
        if resp.status_code == 404 and data["Op"] == "read":
            return
        
    raise RuntimeError('Server is not responding or fails')


def main():
    for _ in range(REQ_NUMBER):
        send_random_request()
    print(check_dict)
main()