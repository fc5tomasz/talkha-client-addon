# Changelog

## 0.2.25
- Bugfix: Dockerfile now uses the official multi-architecture Home Assistant base image as the default `BUILD_FROM`, so Supervisor can build the add-on even when it does not inject `BUILD_FROM` explicitly.

## 0.2.24
- Bugfix: Dockerfile now uses the dynamic Home Assistant `BUILD_FROM` argument, so the add-on can build on the target architecture instead of being pinned to `amd64`.

## 0.2.23
- New: add-on now exposes `operator_url` as a normal configuration field, so the operator endpoint can be changed per client without rebuilding the add-on.
- Bugfix: startup now validates `operator_url` and trims a trailing slash before launching the agent.

## 0.2.22
- Cleanup: `lights-on-report` no longer returns the mixed `aktywnie_swiecace` counter; summary now keeps only `swiatla_wlaczone` and `wizualne_przelaczniki_wlaczone`.

## 0.2.21
- Bugfix: `lights-on-report` now includes real panel key switches again when registry metadata shows a physical light-switch channel such as `L1/L2/Left/Right/Center`, while still excluding helper/backlight/power junk.

## 0.2.20
- Bugfix: `lights-on-report` now reads entity and device registries and excludes logical Zigbee2MQTT `Group` lights, so grouped aliases do not duplicate real physical lights in the report.

## 0.2.19
- Bugfix: `lights-on-report` now ignores `panel_*`, rolety and helper switches; only real lamp/light/LED style switches should remain.

## 0.2.18
- Bugfix: `lights-on-report` now excludes `Multi przelacznik` / `zasilanie` switches, so powered circuits are not mistaken for lit entities.

## 0.2.17
- Bugfix: `lights-on-report` now excludes generic `panel` and `zasilanie` switches, so it focuses on real light-like entities such as `lampki`, `led`, `light` and `oświetlenie`.

## 0.2.16
- Bugfix: `lights-on-report` no longer treats `*_backlight_mode` helper switches as active visual lights; it now prefers real panel/key switches such as `left/right/l1/l2`.

## 0.2.15
- Improved: `lights-on-report` now reports active visual entities, including `light.*` and light-like `switch.*` entities such as panel/backlight states.

## 0.2.14
- New: `lights-on-report` returns all active `light.*` entities with friendly names and timestamps.
- New: `why-light-on --entity-id ...` provides a compact explanation for why a specific light is currently on.
- Bugfix: `przebieg-zdarzen-ha` now accepts time values written with a space, not only ISO `T`.

## 0.2.13
- Bugfix: add-on now requests `hassio_role: manager`, so `zigbee-status-report` can read real Zigbee2MQTT add-ons from Supervisor and report exact bridge names without helper filtering.

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
