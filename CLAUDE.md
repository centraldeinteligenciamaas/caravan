# CLAUDE.md — Projeto Caravan → Supabase

Sincroniza dados das APIs Caravan Fleet para um banco Postgres no Supabase.

## Arquivos
- `caravan_supabase.py` — script principal. DDL (cria tabelas/índices/triggers, idempotente) + sync das APIs. `main()` faz tudo.
- `app.py` — servidor Flask. Agenda o sync via APScheduler (07:00 e 16:00 BRT). Endpoints: `GET /` (health, usado pelo UptimeRobot) e `GET /sync` (dispara sync manual async).
- `.env` — credenciais (NÃO commitar). Chaves: `SUPABASE_HOST`, `SUPABASE_PORTA`, `SUPABASE_BANCO`, `SUPABASE_USUARIO`, `SUPABASE_SENHA`, `CARAVAN_API_KEY`.
- Deploy: Render Web Service (`Procfile` + `render.yaml`), monitorado pelo UptimeRobot.

## "Atualizar o banco do Supabase" = rodar o sync
O script lê das variáveis de ambiente e **não** carrega o `.env` sozinho (sem python-dotenv).

```bash
# no diretório do projeto (Git Bash):
set -a && . ./.env && set +a && PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python caravan_supabase.py
```

## Gotcha: encoding no Windows
Sem `PYTHONIOENCODING=utf-8`, o script quebra na hora de imprimir os emojis (✅/❌) porque o
console usa cp1252 — `UnicodeEncodeError`. **Não é erro de banco.** Sempre rodar com `PYTHONIOENCODING=utf-8`
localmente. No Render (Linux) não acontece.

## Tabelas (Supabase Postgres)
`motoristas`, `escala`, `largadas`, `ferias`, `folga`. Banco com timezone `America/Sao_Paulo`.
Sync usa `INSERT ... ON CONFLICT DO UPDATE` (upsert), então re-execução é segura.
`largadas` sincroniza os últimos `SYNC_DAYS = 30` dias.

## ⚠️ status vs tipo_escala (motoristas) — fonte de divergência
O campo `status` da API `driver-consolidated` vem **sempre "Ativo"** para todos os motoristas —
não diferencia situação nenhuma. A situação real está no campo **`type`** da API
(Fixo / Afastado / Folguista / Ferista / Apoio / Reserva / Nenhum), normalizado por `norm_tipo()`
e gravado na coluna `motoristas.tipo_escala` (valores: FIXO, FOLGUISTA, RESERVA, AFASTADO, APOIO,
FERISTA). **Para saber quem está realmente ativo/afastado/ferista, use `tipo_escala`, NÃO `status`.**
norm_tipo: Nenhum→FIXO (os demais mapeiam 1:1). Mesma lista de valores vale para `escala.tipo_escala`.

Motoristas que somem do feed da API são marcados `status='INATIVO'` no fim do sync (não são
deletados — preserva escala/férias/folgas). Ficam com `tipo_escala` NULL.

## APIs Caravan
Base: `https://api.maas.caravanfleet.com.br/integration`. Auth via header `x-api-key`.
Endpoints usados: `driver-consolidated`, `last-departures-by-date`.

## Ambiente
Python 3.12 (`.python-version`), deps em `requirements.txt` (psycopg2-binary, requests, Flask, APScheduler, gunicorn).
