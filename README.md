# talkha-client-addon

Repozytorium add-onu Home Assistant dla zdalnej pracy serwisowej `TalkHa`.

Autor: Tomasz Furdal, ha-expert.com

Klient:
1. dodaje to repo do Add-on Store,
2. instaluje `Ha-expert-Client`,
3. wpisuje `client_id`, `ha_token` i `operator_url`,
4. uruchamia add-on.

Add-on nie wystawia publicznego portu.
Laczy sie wychodzaco do `talkha-operator-server`.

Po stronie operatora preferowany workflow to wrapper `hx`, ktory daje:
- kompaktowa diagnostyke (`doctor`, `auto-summary`, `script-summary`, `thresholds`, `threshold-check`, `diag-auto`)
- bezpieczne mutacje z backupem i krotkim raportem transakcji (`upsert-automation`, `delete-automation`, `upsert-script`, `delete-script`, `helper-upsert`, `helper-delete`, `tx-summary`, `rollback-tx`)
