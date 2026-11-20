ARG BASE_WORKER_IMAGE
FROM ${BASE_WORKER_IMAGE}
COPY internal_flags.json /var/run/ko/internal_flags.json
