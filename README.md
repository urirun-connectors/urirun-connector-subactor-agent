# urirun-connector-subactor-agent

Natywny connector `urirun`, który kontroluje flotę agentów Subactor wykonującą
walidowane pakiety z `skills-agent`. Connector nie zastępuje orkiestratora zadań:
reużywa jego discovery, kolejności `doctor → repair → validator`, kontraktów JSON
i bramek mutacji.

## Powierzchnia URI

| URI | Efekt |
|---|---|
| `subactor-agent://host/tasks/query/discover` | lista gotowych zadań dla wyzwalacza ręcznego albo cron |
| `subactor-agent://host/trigger/event/emit` | dopisanie idempotentnego triggera ticket/repository/webhook |
| `subactor-agent://host/cycle/command/run` | jeden cykl discovery → deduplikacja → wykonanie → feedback |
| `subactor-agent://host/loop/session/run` | ograniczona pętla: najpierw kolejka triggerów, potem cron |
| `subactor-agent://host/state/query/status` | generacja, kolejka i health/backoff zadań |
| `subactor-agent://host/runs/query/list` | ograniczona historia receiptów wykonania |
| `subactor-agent://host/doctor/query/report` | gotowość zależności, repozytorium i bramek mutacji |

`execute=false` jest wartością domyślną. Payload nigdy nie podaje komendy, ścieżki
repozytorium ani zmiennych środowiskowych wykonawcy. Repozytorium źródłowe ustala
operator przez `SUBACTOR_SKILLS_ROOT`.

## Instalacja i konfiguracja

```bash
python -m pip install 'urirun-connector-subactor-agent[skills] @ git+https://github.com/urirun-connectors/urirun-connector-subactor-agent.git'

export SUBACTOR_SKILLS_ROOT=/srv/subactor/skills-agent
export SUBACTOR_AGENT_STATE_DIR=/var/lib/urirun/subactor-agent
export SUBACTOR_AGENT_ENABLED=true
```

Opcjonalne granice operatora:

| zmienna | domyślnie | znaczenie |
|---|---:|---|
| `SUBACTOR_AGENT_ALLOW_APPLY` | `false` | zezwala na `apply_changes=true`; nadal obowiązuje polityka taska `skills-agent` |
| `SUBACTOR_AGENT_MAX_TASKS` | `10` | maksymalna liczba faktycznie uruchomionych tasków na cykl |
| `SUBACTOR_AGENT_MAX_CYCLES` | `100` | maksymalna liczba iteracji jednej sesji loop |
| `SUBACTOR_AGENT_RETRY_BASE_SECONDS` | `60` | początek wykładniczego backoff |
| `SUBACTOR_AGENT_RETRY_MAX_SECONDS` | `3600` | górna granica backoff |

Automatyczny backlog Planfile/OneDev włącza pięć zmiennych operatora:

```bash
export SUBACTOR_PLANFILE_BACKEND=onedev
export SUBACTOR_PLANFILE_ONEDEV_URL=http://onedev:6610
export SUBACTOR_PLANFILE_ONEDEV_PROJECT=subactor/skills-agent
export SUBACTOR_PLANFILE_ONEDEV_USER=onedev-admin
export SUBACTOR_PLANFILE_ONEDEV_PASSWORD_FILE=/run/secrets/onedev-admin-password
```

Connector nie implementuje API OneDev drugi raz. Reużywa
`planfile.sync.OneDevBackend` udostępniony przez `skills-agent`, synchronizuje
zadania po trwałym markerze `todo-task:<task_id>`, a następnie wybiera pierwszy
otwarty ticket zgodny z kontraktem `ready`. Dopiero cykl z `execute=true`
ustawia ticket jako `In Progress`; receipt `succeeded`, `warning` i `failed`
przechodzi odpowiednio do `Closed`, `In Review` i ponownie `Open`. Podgląd
`execute=false` nie zmienia statusu ticketu.

Błąd kontraktu, bezpieczeństwa lub zakresu celu nie jest retryowany w pętli:
ticket przechodzi do `In Review`, a kolejka może podjąć następne zadanie. Błędy
wykonawcze i zależności pozostają retryowalne (`Open` + backoff).

Odroczony retry nie blokuje kolejki: jego ticket pozostaje `Open`, ale connector
wyklucza go z kolejnego wyboru do czasu `retry_at` i podejmuje następny otwarty
ticket. Gdy Planfile nie ma pracy, connector raportuje jawny stan idle i nie
wraca do lokalnego harmonogramu, który mógłby ponownie wykonać zakończone zadanie.
Blokady są utrwalane jako `subactor.resolution.blocker/v1` wraz z decyzją
`continue_unblocked`, zgodną z polityką ciągłości `subactor/orchestrator`.

Stan jest atomowym JSON-em. Przechowuje generację, idempotency receipts, kolejkę
triggerów, aktywne typowane blokady, ostatnie 200 wyników i licznik porażek. Nie
przechowuje promptów, tokenów ani pełnych logów agentów; artefakty pozostają w
`.skills-agent-runs`.

## Przykładowy przepływ

```bash
# kontrola konfiguracji
urirun-subactor-agent report

# podgląd cyklu cron bez wykonania
urirun-subactor-agent cycle-run --trigger schedule

# wyzwalacz z biletu (event_id zapewnia deduplikację)
urirun-subactor-agent emit \
  --kind ticket --event_id planfile-PLF-123 --task_id 0042_customer-mail-triage

# jedno wykonanie kolejki; mutacje nadal są wyłączone
urirun-subactor-agent loop-session-run --max_cycles 1 --execute
```

Zewnętrzny cron powinien wywoływać pojedynczy `cycle/command/run` albo krótką
`loop/session/run`. Slot harmonogramu z `skills-agent` jest częścią klucza receipt,
więc ponowne wywołanie tego samego slotu nie uruchomi taska drugi raz.

## „Infrastruktura ewolucyjna”

Ewolucja oznacza tutaj zamkniętą, audytowalną pętlę informacji zwrotnej:

`ticket/trigger → task contract → doctor → repair → validator → receipt → health/backoff → następna generacja`.

Planfile i trwały stan connectora pozostają właścicielami backlogu i retry.
`subactor/orchestrator` jest warstwą polityki decyzji (authority, provider,
continuity), ale nie zastępuje schedulera: obecnie nie utrwala własnych prób
strategii po restarcie. Connector zapisuje kompatybilny kontrakt blokady tam,
gdzie trwałość już istnieje; bezpośrednie wywołanie orchestratora można dołączyć
po udostępnieniu jego stabilnego runtime API i trwałego rejestru prób.

Connector nie zmienia samodzielnie kodu, allowlist ani polityki. Zmiana produktu
jest możliwa wyłącznie jako jawny task dopuszczony przez kontrakt `skills-agent`, a
`apply_changes` wymaga dwóch niezależnych bramek: konfiguracji connectora i
polityki zadania.

## Rozwój

```bash
python -m pip install -e '.[test]'
ruff check .
python -m pytest
python -m build
```

Licencja: Apache-2.0.
