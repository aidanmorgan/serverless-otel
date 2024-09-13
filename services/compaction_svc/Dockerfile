FROM public.ecr.aws/lambda/python:3.11
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY libs/ {LAMBDA_TASK_ROOT}
COPY services/compaction_svc/requirements-aws.txt {LAMBDA_TASK_ROOT}
RUN python -m pip install -r {LAMBDA_TASK_ROOT}/requirements-aws.txt

CMD ["lambda_function.lambda_handler"]
