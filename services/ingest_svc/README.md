
To create docker image for AWS lambda, run from the top-level directory

```
docker build -f ./services/ingest_svc/Dockerfile -t serverlessotel-ingest:latest --platform linux/amd64  .
```