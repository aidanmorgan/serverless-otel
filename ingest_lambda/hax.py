import time
from typing import Final
from uuid import uuid4

import tempfile
import ingest_lambda, s3_mutex

# this is just a play script to try out some ideas, please don't think this is good

NUM_ITERATIONS: Final[int] = 1500000

if __name__ == '__main__':
    ingest_lambda.__STORAGE_BASE_PATH__ = tempfile.mkdtemp()
    ingest_lambda.__USE_S3_MUTEX__ = True
    ingest_lambda.__USE_FILESYSTEM_MUTEX__ = False

    s3_mutex.__BUCKET_NAME__ = 'dev-serverless-otel-segments'
    s3_mutex.__PROFILE_NAME__ = 'aidan-personal'


    for i in range(0, NUM_ITERATIONS):
        ingest_lambda.lambda_handler({
            "body": f'timestamp-ns={time.time_ns()}\ncorrelation-id={uuid4().hex}\ndataset-id=fake-dataset-1\n{'\n'.join(["field-1=1", "field-2=2", "field3=3", "field4=4"])}'
        }, None)