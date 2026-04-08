# Changelog

## 0.2.2
- Bugfix: add-on now connects to local Home Assistant Core through `http://homeassistant:8123` instead of `supervisor/core`, so the HA user token works correctly inside the container.

## 0.2.1
- Bugfix: auto-correct swapped `ha_token` and `client_id` fields if Home Assistant saves them in reverse order.

## 0.2.0
- New name: `Ha-expert-Client`
- Simpler setup: fixed operator endpoint and shared registration token
- Simpler setup: local HA URL handled automatically inside add-on
- Bugfix: correct Home Assistant config paths inside add-on
- Bugfix: stable image build with internal virtual environment

## 0.1.3
- Author text changed to `ha-expert.com`

## 0.1.1
- Bugfix: add-on runtime uses `/homeassistant` paths

## 0.1.0
- Initial working release
