import time
from concurrent.futures.process import ProcessPoolExecutor
from random import random
from typing import Final, List
from uuid import uuid4

import lorem
import requests
import random

__API_URL__ : Final[str] = 'https://trvkgujvoa.execute-api.us-east-1.amazonaws.com/prod/ingest'

__FIELD_NAMES__: List[str] = ['service_name.varchar', 'operation_name.varchar', 'status_code.int64', 'http_method.varchar', 'http_url.varchar', 'http_status_code.int64', 'queue_name.varchar', 'message_id.varchar', 'duration.float64', 'user_id.int64', 'session_id.varchar', 'region.varchar', 'instance_id.int64', 'environment.varchar', 'version.float64']
__DATASET_NAMES__: List[str] = ['fake-data-1', 'fake-data-2', 'fake-data-3']
__MAX_PARALLEL__: Final[int] = 150
__MAX_REQUESTS__: Final[int] = 50000
NS_PER_MS: Final[int] = 1000000


from requests import Response


def make_payload() -> str:
    fields: List[str] = []

    field:str
    for field in random.sample(__FIELD_NAMES__, random.randint(1, len(__FIELD_NAMES__))):
        if field.endswith('varchar'):
            fields.append(f'{field}={lorem.text()}')
        elif field.endswith('int64'):
            fields.append(f'{field}={random.randint(-50000, 50000)}')
        elif field.endswith('float64'):
            fields.append(f'{field}={random.random()}')

    return f'timestamp-ns={time.time_ns()}\ncorrelation-id={uuid4().hex}\ndataset-id={random.choice(__DATASET_NAMES__)}\n{'\n'.join(fields)}'

def run_one_loop():
    for i in range(1, __MAX_REQUESTS__):
        data:str = make_payload()

        start: int = time.time_ns()
        x: Response = requests.post(__API_URL__, data=data, headers={'Content-Type': 'text/plain; charset=utf-8'})
        end: int = time.time_ns()

        print(f'POST: {data.count('=') - 3} fields. Response: {x.status_code}. Took: {(end-start) / NS_PER_MS} ns')

if __name__ == '__main__':
    with ProcessPoolExecutor() as executor:
        for process in range(0, __MAX_PARALLEL__):
            executor.submit(run_one_loop)


