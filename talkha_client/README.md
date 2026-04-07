# TalkHa Client Add-on

Pelna wersja `TalkHa` po stronie klienta.

Zadania:
- runtime diagnoza przez `TalkHa.py`
- konfiguracja i mutacje przez `TalkHaLokal.py`
- jedno stale polaczenie wychodzace do serwera operatora

## Zasada

Ten add-on bazuje na skopiowanych, sprawdzonych oryginalnych skryptach:
- `/home/tomasz/HA/tools/TalkHa.py`
- `/home/tomasz/HA/TalkHaLokal/TalkHaLokal.py`

Nowa jest tylko warstwa opakowania:
- repo add-onu
- agent klienta
- serwer operatora
- dostosowanie sciezek do add-onu (`/config`, `/data`, `/opt/talkha`)

## Instalacja u klienta

1. Dodac repo add-onow.
2. Zainstalowac `TalkHa Client`.
3. Wpisac:
   - `ha_url`
   - `ha_token`
   - `operator_url`
   - `client_id`
   - `registration_token`
4. Uruchomic add-on.

## Model lacznosci

- add-on klienta nie wystawia publicznego portu
- add-on sam laczy sie wychodzaco do serwera operatora
- operator wysyla zadania przez swoj serwer, nie bezposrednio do IP klienta
- nie wymaga to VPN ani przekierowania portow
