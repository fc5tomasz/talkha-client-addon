# Changelog

## 0.2.12
- Improved: `zigbee-status-report` now maps real Zigbee2MQTT add-ons from Supervisor, returns exact add-on names and separates orphan Zigbee bridge entities from real active bridges.

## 0.2.11
- New: `zigbee-status-report` returns Zigbee bridge online/offline summary and offline Zigbee entities without manual filtering.

## 0.2.10
- UI/Docs: renamed remaining `TalkHa Client` labels to `Ha-expert-Client`.

## 0.2.9
- New: `get-entity --entity-id ...` returns a single entity state directly, without reading and filtering the full states dump.

## 0.2.8
- New: `diagnoza-automatyzacji` now returns `analiza_slotow` for supported scheduled automations, including blocked slots and likely blocking conditions.

## 0.2.7
- New: `diagnoza-automatyzacji` now returns `uruchomienia` with logbook-based run counts in the selected time window.
- Bugfix: `diagnoza-automatyzacji` now exposes the correct state, timeline and missing-evidence fields.

## 0.2.6
- New: `diagnoza-automatyzacji` for practical per-client automation diagnosis by alias or id.

## 0.2.5
- New rule: when adding a new script, `alias` is required.
- New rule: when adding a new automation, `alias` is required.
- Automation-oriented read and upsert flows now default to alias-first matching.

## 0.2.4
- New: `get-script` returns the full YAML block of a script by key or alias.
- New: `get-automation` returns the full YAML block of an automation by id or alias.

## 0.2.3
- New: `TalkHaLokal upsert-script` and `upsert-automation` accept `--block-base64`, so operator can send YAML payload directly without SSH and without creating helper files on the client host.

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
