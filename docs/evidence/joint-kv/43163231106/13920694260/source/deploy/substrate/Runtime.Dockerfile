FROM python:3.13-slim@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /opt/agentcloud
COPY runtime /opt/agentcloud/runtime
EXPOSE 80
ENTRYPOINT ["python3", "-m", "runtime.tools_server", "--root", "/workspace", "--host", "0.0.0.0", "--port", "80", "--snapshot-fidelity", "VOLUMES,ROOTFS,MEMORY"]
