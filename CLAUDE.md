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
FERISTA, NENHUM). **Para saber quem está realmente ativo/afastado/ferista, use `tipo_escala`, NÃO `status`.**
norm_tipo: os tipos com escala mapeiam 1:1; **Nenhum→NENHUM** (os demais 1:1). Mesma lista vale para `escala.tipo_escala`.

## ⚠️ NENHUM = motorista sem escala + limite da API
A `driver-consolidated` retorna **todos os motoristas com escala atribuída** (Fixo/Folguista/Reserva/
Afastado/Apoio/Ferista — contagens batem exatamente com o sistema e são **estáveis** entre chamadas) +
um **punhado flutuante** dos "Nenhum" (não-atribuídos). Os ~300 "Nenhum" restantes a API **nunca expõe**
— não há endpoint para a frota completa (todos os candidatos dão 404; `?all=true` é ignorado). Logo a
contagem de NENHUM **nunca vai bater** com o sistema (limitação da API, não tem conserto).

Como os tipos com escala são estáveis, **"ausente do feed" ⇔ "Nenhum"**. No fim do sync, todo motorista
do banco que não veio no feed é marcado `tipo_escala='NENHUM'` (e `status='ATIVO'`). Isso: (a) limpa os
"Nenhum" antigos que ficavam presos como FIXO; (b) faz **todos os tipos com escala baterem com o sistema**.
**Consulte a frota "real" com `WHERE tipo_escala <> 'NENHUM'`** — o balde NENHUM é ruído e deve ser ignorado.

Nada é deletado (escala/férias/folgas preservadas). **Não há mais marcação automática de `INATIVO`**: a API
não expõe sinal de desligamento, então ausência do feed ≠ demissão (antes marcava INATIVO e oscilava).

**Salvaguarda contra feed curto** (`RECLASS_MIN_RATIO = 0.9`): a regra "ausente→NENHUM" é cega — se a API
engasgar e devolver um feed curto, varreria assigned reais para NENHUM. Por isso a reclassificação só roda
se o nº de assigned (tipo ≠ NENHUM) que veio no feed for ≥ 90% dos assigned que o banco já conhece. Feed
curto ⇒ reclassificação pulada + aviso no log (upsert normal acontece de qualquer forma).

## APIs Caravan
Base: `https://api.maas.caravanfleet.com.br/integration`. Auth via header `x-api-key`.
Endpoints usados: `driver-consolidated`, `last-departures-by-date`.

## Ambiente
Python 3.12 (`.python-version`), deps em `requirements.txt` (psycopg2-binary, requests, Flask, APScheduler, gunicorn).
