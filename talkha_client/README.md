# Ha-expert-Client Add-on

Zdalne narzedzie serwisowe dla Home Assistant.

Autor: Tomasz Furdal, ha-expert.com

## Instalacja u klienta

1. Dodac repo add-onow.
2. Zainstalowac `Ha-expert-Client`.
3. Wpisac:
   - `ha_token`
   - `client_id`
   - `operator_url`
4. Uruchomic add-on.

## Dzialanie

- add-on laczy sie z serwerem serwisowym
- nie wymaga publicznego portu po stronie klienta
- moze byc uzywany do diagnozy i prac serwisowych
- `operator_url` wskazuje osiagalny adres operatora, np. tunel, domene albo publiczny endpoint
- po stronie operatora warto po instalacji uruchomic kontrolny test `hx doctor`
- zdalny runtime udostepnia teraz tez `get-state`, `last-trigger`, `state-history`, `recent-changes`, `set-helper` oraz `where-used`
- zdalny runtime udostepnia teraz tez `automation-summary` i `script-summary`, aby operator mogl szybko zobaczyc triggery, warunki i akcje bez czytania pelnego YAML
- zdalny runtime udostepnia tez `entity-thresholds` i `threshold-check`, aby sprawdzac progi temperatur i ryzyko kolizji bez recznego skladania kilku komend
- `diagnoza-automatyzacji` rozbija teraz sloty zarowno dla harmonogramow `time_pattern`, jak i dla stalych triggerow `time`
- `diagnoza-automatyzacji` wykrywa teraz tez przypadek `numeric_state`, gdy prog byl juz spelniony zanim pozostale warunki zrobily sie prawdziwe
