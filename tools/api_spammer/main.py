import json
import os
import time
from concurrent.futures.process import ProcessPoolExecutor
from random import random
from typing import Final, List, Dict, Optional
from uuid import uuid4

import lorem
import requests
import random


FIELD_NAMES: Final[List[str]] = ['service_name.varchar', 'operation_name.varchar', 'status_code.int64', 'http_method.varchar', 'http_url.varchar', 'http_status_code.int64', 'queue_name.varchar', 'message_id.varchar', 'duration.float64', 'user_id.int64', 'session_id.varchar', 'region.varchar', 'instance_id.int64', 'environment.varchar', 'version.float64']
DATASET_NAMES: Final[List[str]] = ['fake-data-1', 'fake-data-2', 'fake-data-3']
MAX_PROCESSES: Final[int] = 1
REQUESTS_PER_PROCESS: Final[int] = 5
NS_PER_MS: Final[int] = 1000000

API_URL : Final[str] = os.getenv('API_URL')
USE_API_GATEWAY: Final[bool] = True


from requests import Response


def make_payload() -> str:
    fields: List[str] = []

    field:str
    for field in random.sample(FIELD_NAMES, random.randint(1, len(FIELD_NAMES))):
        if field.endswith('varchar'):
            fields.append(f'{field}={lorem.text()}')
        elif field.endswith('int64'):
            fields.append(f'{field}={random.randint(-50000, 50000)}')
        elif field.endswith('float64'):
            fields.append(f'{field}={random.random()}')

    return f'timestamp-ns={time.time_ns()}\ncorrelation-id={uuid4().hex}\ndataset-id={random.choice(DATASET_NAMES)}\n{'\n'.join(fields)}'

def run_one_loop():
    for i in range(0, REQUESTS_PER_PROCESS):
        data:str = make_payload()

        start: int = time.time_ns()

        response: Optional[Response] = None

        if USE_API_GATEWAY:
            dict: Dict[str, str] = {
                'body': data,
            }

            response = requests.post(API_URL, data=json.dumps(dict), headers={'Content-Type': 'application/json; charset=utf-8'})
        else:
            response = requests.post(API_URL, data=str.encode(data), headers={'Content-Type': 'text/plain; charset=utf-8'})

        end: int = time.time_ns()

        print(f'POST: {data.count('=') - 3} fields. Response: {response.status_code}. Took: {(end-start) / NS_PER_MS} ms')

if __name__ == '__main__':
    # we want to send as many as possible requests in parallel to make the concurrency of the receiving lambda go up
    with ProcessPoolExecutor() as executor:
        for process in range(0, MAX_PROCESSES):
            executor.submit(run_one_loop)


