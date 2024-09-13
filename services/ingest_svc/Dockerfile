FROM public.ecr.aws/lambda/python:3.11
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY libs/ .
COPY services/ingest_svc/ .
RUN python -m pip install -r ./requirements-aws.txt

CMD ["lambda_function.lambda_handler"]
