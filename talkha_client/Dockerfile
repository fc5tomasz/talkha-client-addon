ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache bash curl mariadb-client openssh-client py3-pip
RUN python3 -m venv /opt/talkha/venv \
    && /opt/talkha/venv/bin/python -m pip install --no-cache-dir aiohttp PyYAML websockets

COPY rootfs /

RUN chmod +x /etc/services.d/talkha-client/run \
    && chmod +x /opt/talkha/app/agent.py \
    && chmod +x /opt/talkha/runtime/TalkHa.py \
    && chmod +x /opt/talkha/runtime/TalkHaLokal.py
