"""
caravan_supabase.py
Cria/migra as tabelas do Caravan no Supabase e sincroniza os dados das APIs.

Uso local:
    export $(cat .env | xargs)
    python caravan_supabase.py

Via GitHub Actions:
    Cron: 7h e 17h (BRT) — workflow .github/workflows/main.yml
"""

import os
import sys
import requests
import psycopg2
from datetime import datetime, timezone, timedelta


# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ──────────────────────────────────────────────────────────────────────────────

API_KEY   = os.environ.get("CARAVAN_API_KEY")
API_BASE  = "https://api.maas.caravanfleet.com.br/integration"
HEADERS   = {"x-api-key": API_KEY} if API_KEY else {}

BRT       = timezone(timedelta(hours=-3))
SYNC_DAYS = 30   # janela de sync de largadas (últimos N dias)


def get_connection():
    config = {
        "host":            os.environ.get("SUPABASE_HOST"),
        "port":            os.environ.get("SUPABASE_PORTA",  "5432"),
        "dbname":          os.environ.get("SUPABASE_BANCO",  "postgres"),
        "user":            os.environ.get("SUPABASE_USUARIO"),
        "password":        os.environ.get("SUPABASE_SENHA"),
        "sslmode":         "require",
        "connect_timeout": 10,
    }
    obrigatorias = {
        "SUPABASE_HOST":    config["host"],
        "SUPABASE_USUARIO": config["user"],
        "SUPABASE_SENHA":   config["password"],
        "CARAVAN_API_KEY":  API_KEY,
    }
    faltando = [var for var, val in obrigatorias.items() if not val]
    if faltando:
        for var in faltando:
            print(f"  ❌ Variável não encontrada: {var}")
        sys.exit(1)
    return psycopg2.connect(**config)


# ──────────────────────────────────────────────────────────────────────────────
# DDL — cada item é UMA statement SQL (sem encadeamento)
# Usar statements separadas evita que um COMMENT quebre uma CREATE TABLE
# que já existia. Savepoints garantem que um erro não aborta os demais.
# ──────────────────────────────────────────────────────────────────────────────

STATEMENTS = [

    # ── MOTORISTAS ────────────────────────────────────────────────────────────
    {"label": "tabela: motoristas", "sql": """
        CREATE TABLE IF NOT EXISTS motoristas (
            id          SERIAL       PRIMARY KEY,
            chapa       VARCHAR(20)  NOT NULL UNIQUE,
            nome        VARCHAR(200) NOT NULL,
            funcao      VARCHAR(100),
            status      VARCHAR(20)  NOT NULL DEFAULT 'ATIVO'
                            CHECK (status IN ('ATIVO', 'INATIVO', 'AFASTADO')),
            created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        )
    """},
    {"label": "comment: motoristas", "sql": """
        COMMENT ON TABLE motoristas IS 'Cadastro de motoristas da frota Caravan'
    """},

    # ── ESCALA ────────────────────────────────────────────────────────────────
    {"label": "tabela: escala", "sql": """
        CREATE TABLE IF NOT EXISTS escala (
            id                 SERIAL       PRIMARY KEY,
            motorista_id       INTEGER      NOT NULL
                                   REFERENCES motoristas(id) ON DELETE CASCADE,
            numero_contrato    VARCHAR(20)  NOT NULL,
            cliente            VARCHAR(200),
            turno              VARCHAR(30)  NOT NULL
                                   CHECK (turno IN (
                                       'PRIMEIRO TURNO','SEGUNDO TURNO','TERCEIRO TURNO'
                                   )),
            tipo_escala        VARCHAR(20)  NOT NULL DEFAULT 'FIXO'
                                   CHECK (tipo_escala IN (
                                       'FIXO','FOLGUISTA','RESERVA','AFASTADO','APOIO'
                                   )),
            prefixo            VARCHAR(20),
            placa              VARCHAR(20),
            modelo_veiculo     VARCHAR(100),
            local_apresentacao VARCHAR(200),
            hora_inicio        TIME,
            hora_fim           TIME,
            created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            CONSTRAINT escala_motorista_contrato_unica
                UNIQUE (motorista_id, numero_contrato)
        )
    """},

    # Migração: adiciona colunas novas em escala já existente
    {"label": "migração: escala.placa", "sql": """
        ALTER TABLE escala ADD COLUMN IF NOT EXISTS placa VARCHAR(20)
    """},
    {"label": "migração: escala.modelo_veiculo", "sql": """
        ALTER TABLE escala ADD COLUMN IF NOT EXISTS modelo_veiculo VARCHAR(100)
    """},
    {"label": "migração: escala.hora_inicio", "sql": """
        ALTER TABLE escala ADD COLUMN IF NOT EXISTS hora_inicio TIME
    """},
    {"label": "migração: escala.hora_fim", "sql": """
        ALTER TABLE escala ADD COLUMN IF NOT EXISTS hora_fim TIME
    """},
    {"label": "migração: escala.constraint_unica", "sql": """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'escala_motorista_contrato_unica'
                  AND conrelid = 'escala'::regclass
            ) THEN
                ALTER TABLE escala ADD CONSTRAINT escala_motorista_contrato_unica
                    UNIQUE (motorista_id, numero_contrato);
            END IF;
        END $$
    """},

    # ── LARGADAS ──────────────────────────────────────────────────────────────
    {"label": "tabela: largadas", "sql": """
        CREATE TABLE IF NOT EXISTS largadas (
            id                 SERIAL       PRIMARY KEY,
            departure_id       INTEGER      UNIQUE,
            shift_name         VARCHAR(200),
            driver_name        VARCHAR(200),
            plate              VARCHAR(20),
            prefixo            VARCHAR(20),
            vehicle_model      VARCHAR(100),
            number_contract    VARCHAR(20),
            supplier_customer  VARCHAR(200),
            trade_name         VARCHAR(200),
            local_apresentacao VARCHAR(200),
            start_time         TIME,
            end_time           TIME,
            departure_datetime TIMESTAMPTZ,
            synced_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        )
    """},

    # ── FERIAS ────────────────────────────────────────────────────────────────
    {"label": "tabela: ferias", "sql": """
        CREATE TABLE IF NOT EXISTS ferias (
            id           SERIAL       PRIMARY KEY,
            motorista_id INTEGER      NOT NULL
                             REFERENCES motoristas(id) ON DELETE CASCADE,
            api_id       INTEGER      UNIQUE,
            data_inicio  DATE         NOT NULL,
            data_fim     DATE         NOT NULL,
            dias_gozados INTEGER      GENERATED ALWAYS AS (
                             (data_fim - data_inicio + 1)
                         ) STORED,
            substituto_id INTEGER     REFERENCES motoristas(id) ON DELETE SET NULL,
            publish_date DATE,
            created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            CONSTRAINT ferias_datas_validas CHECK (data_fim >= data_inicio)
        )
    """},
    # Migração: adiciona api_id caso a tabela ferias já exista sem ela
    {"label": "migração: ferias.api_id", "sql": """
        ALTER TABLE ferias ADD COLUMN IF NOT EXISTS api_id INTEGER
    """},
    {"label": "migração: ferias.api_id unique", "sql": """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'ferias_api_id_key' AND conrelid = 'ferias'::regclass
            ) THEN
                ALTER TABLE ferias ADD CONSTRAINT ferias_api_id_key UNIQUE (api_id);
            END IF;
        END $$
    """},
    {"label": "migração: ferias.substituto_id", "sql": """
        ALTER TABLE ferias ADD COLUMN IF NOT EXISTS substituto_id INTEGER
            REFERENCES motoristas(id) ON DELETE SET NULL
    """},
    {"label": "migração: ferias.publish_date", "sql": """
        ALTER TABLE ferias ADD COLUMN IF NOT EXISTS publish_date DATE
    """},

    # ── FOLGA ─────────────────────────────────────────────────────────────────
    {"label": "tabela: folga", "sql": """
        CREATE TABLE IF NOT EXISTS folga (
            id            SERIAL       PRIMARY KEY,
            motorista_id  INTEGER      NOT NULL
                              REFERENCES motoristas(id) ON DELETE CASCADE,
            api_id        INTEGER      UNIQUE,
            data_folga    DATE         NOT NULL,
            substituto_id INTEGER      REFERENCES motoristas(id) ON DELETE SET NULL,
            publish_date  DATE,
            created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            CONSTRAINT folga_unica_por_dia UNIQUE (motorista_id, data_folga)
        )
    """},
    # Migração: adiciona api_id caso a tabela folga já exista sem ela
    {"label": "migração: folga.api_id", "sql": """
        ALTER TABLE folga ADD COLUMN IF NOT EXISTS api_id INTEGER
    """},
    {"label": "migração: folga.api_id unique", "sql": """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'folga_api_id_key' AND conrelid = 'folga'::regclass
            ) THEN
                ALTER TABLE folga ADD CONSTRAINT folga_api_id_key UNIQUE (api_id);
            END IF;
        END $$
    """},
    {"label": "migração: folga.substituto_id", "sql": """
        ALTER TABLE folga ADD COLUMN IF NOT EXISTS substituto_id INTEGER
            REFERENCES motoristas(id) ON DELETE SET NULL
    """},
    {"label": "migração: folga.publish_date", "sql": """
        ALTER TABLE folga ADD COLUMN IF NOT EXISTS publish_date DATE
    """},

    # ── ÍNDICES ───────────────────────────────────────────────────────────────
    {"label": "índice: motoristas.chapa",    "sql": "CREATE INDEX IF NOT EXISTS idx_motoristas_chapa    ON motoristas(chapa)"},
    {"label": "índice: motoristas.nome",     "sql": "CREATE INDEX IF NOT EXISTS idx_motoristas_nome     ON motoristas(nome)"},
    {"label": "índice: escala.motorista_id", "sql": "CREATE INDEX IF NOT EXISTS idx_escala_motorista    ON escala(motorista_id)"},
    {"label": "índice: escala.contrato",     "sql": "CREATE INDEX IF NOT EXISTS idx_escala_contrato     ON escala(numero_contrato)"},
    {"label": "índice: largadas.datetime",   "sql": "CREATE INDEX IF NOT EXISTS idx_largadas_dt         ON largadas(departure_datetime DESC)"},
    {"label": "índice: largadas.contrato",   "sql": "CREATE INDEX IF NOT EXISTS idx_largadas_contrato   ON largadas(number_contract)"},
    {"label": "índice: ferias.motorista_id", "sql": "CREATE INDEX IF NOT EXISTS idx_ferias_motorista    ON ferias(motorista_id)"},
    {"label": "índice: ferias.datas",        "sql": "CREATE INDEX IF NOT EXISTS idx_ferias_datas        ON ferias(data_inicio, data_fim)"},
    {"label": "índice: folga.motorista_id",  "sql": "CREATE INDEX IF NOT EXISTS idx_folga_motorista     ON folga(motorista_id)"},
    {"label": "índice: folga.data",          "sql": "CREATE INDEX IF NOT EXISTS idx_folga_data          ON folga(data_folga)"},

    # ── TRIGGER updated_at ────────────────────────────────────────────────────
    {"label": "função: set_updated_at", "sql": """
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
        $$ LANGUAGE plpgsql
    """},
    {"label": "trigger: motoristas.updated_at", "sql": """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_motoristas_updated_at') THEN
                CREATE TRIGGER trg_motoristas_updated_at BEFORE UPDATE ON motoristas
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
            END IF;
        END $$
    """},
    {"label": "trigger: escala.updated_at", "sql": """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_escala_updated_at') THEN
                CREATE TRIGGER trg_escala_updated_at BEFORE UPDATE ON escala
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
            END IF;
        END $$
    """},
    {"label": "trigger: ferias.updated_at", "sql": """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_ferias_updated_at') THEN
                CREATE TRIGGER trg_ferias_updated_at BEFORE UPDATE ON ferias
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
            END IF;
        END $$
    """},
    {"label": "trigger: folga.updated_at", "sql": """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_folga_updated_at') THEN
                CREATE TRIGGER trg_folga_updated_at BEFORE UPDATE ON folga
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
            END IF;
        END $$
    """},
]


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def log(msg):
    print(f"  {msg}")

def separador(char="─", n=55):
    print(char * n)

def api_get(path, params=None):
    url = f"{API_BASE}/{path}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def normalizar_turno(shift_name):
    s = (shift_name or "").lower()
    if "primeiro" in s or "1" in s: return "PRIMEIRO TURNO"
    if "segundo"  in s or "2" in s: return "SEGUNDO TURNO"
    if "terceiro" in s or "3" in s: return "TERCEIRO TURNO"
    return "PRIMEIRO TURNO"

def normalizar_tipo(tipo):
    return {
        "fixo": "FIXO", "folguista": "FOLGUISTA", "reserva": "RESERVA",
        "afastado": "AFASTADO", "apoio": "APOIO",
        "ferista": "AFASTADO", "nenhum": "FIXO",
    }.get((tipo or "").lower(), "FIXO")

def normalizar_status(status):
    s = (status or "").lower()
    if "ativo"   in s: return "ATIVO"
    if "inativo" in s: return "INATIVO"
    return "AFASTADO"


# ──────────────────────────────────────────────────────────────────────────────
# ETAPA 1 — DDL com savepoints (um erro não aborta os demais)
# ──────────────────────────────────────────────────────────────────────────────

def criar_tabelas(cur):
    separador("=")
    print("  ETAPA 1 — Estrutura das tabelas")
    separador("=")

    avisos = []
    for i, item in enumerate(STATEMENTS, 1):
        try:
            cur.execute("SAVEPOINT ddl_sp")
            cur.execute(item["sql"])
            cur.execute("RELEASE SAVEPOINT ddl_sp")
            log(f"✅ [{i:02d}] {item['label']}")
        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT ddl_sp")
            # Avisos esperados: objeto já existe, constraint já existe, etc.
            msg = str(e).strip().replace("\n", " ")
            log(f"⚠️  [{i:02d}] {item['label']} → {msg}")
            avisos.append(item["label"])

    if avisos:
        log(f"\n  ℹ️  {len(avisos)} statement(s) puladas (objetos já existentes ou incompatíveis)")
    return []   # nunca aborta por DDL — avisos são esperados em re-execuções


# ──────────────────────────────────────────────────────────────────────────────
# ETAPA 2 — SYNC MOTORISTAS + ESCALA + FÉRIAS + FOLGAS
# ──────────────────────────────────────────────────────────────────────────────

def sync_drivers(cur):
    separador("=")
    print("  ETAPA 2 — Sync motoristas / escala / férias / folgas")
    separador("=")

    log("🌐 Buscando driver-consolidated...")
    dados = api_get("driver-consolidated")
    log(f"   {len(dados)} motoristas recebidos\n")

    mot_ins = mot_upd = esc_ins = esc_upd = 0
    fer_ins = fer_upd = fol_ins = fol_upd = 0

    for item in dados:
        drv = item["driver"]
        chapa = str(drv["employeeId"])

        # ── Motorista ──────────────────────────────────────────────────────
        cur.execute("""
            INSERT INTO motoristas (chapa, nome, funcao, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chapa) DO UPDATE SET
                nome       = EXCLUDED.nome,
                funcao     = EXCLUDED.funcao,
                status     = EXCLUDED.status,
                updated_at = NOW()
            RETURNING (xmax = 0) AS inserido
        """, (
            chapa,
            drv["name"],
            drv.get("position"),
            normalizar_status(drv.get("status", "Ativo")),
        ))
        if cur.fetchone()[0]: mot_ins += 1
        else:                 mot_upd += 1

        cur.execute("SELECT id FROM motoristas WHERE chapa = %s", (chapa,))
        mot_id = cur.fetchone()[0]

        # ── Escala ─────────────────────────────────────────────────────────
        sched = item.get("regularSchedule") or {}
        if sched:
            veiculo  = sched.get("vehicle") or {}
            shift    = sched.get("shift") or drv.get("shift") or {}
            loc      = sched.get("startLocation") or {}
            turno    = normalizar_turno(shift.get("shiftName", ""))
            contrato = drv.get("contractNumber", "")
            modelo   = " - ".join(filter(None, [
                veiculo.get("brandName"), veiculo.get("modelName")
            ])) or None

            cur.execute("""
                INSERT INTO escala (
                    motorista_id, numero_contrato, cliente, turno, tipo_escala,
                    prefixo, placa, modelo_veiculo, local_apresentacao,
                    hora_inicio, hora_fim
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (motorista_id, numero_contrato) DO UPDATE SET
                    cliente            = EXCLUDED.cliente,
                    turno              = EXCLUDED.turno,
                    tipo_escala        = EXCLUDED.tipo_escala,
                    prefixo            = EXCLUDED.prefixo,
                    placa              = EXCLUDED.placa,
                    modelo_veiculo     = EXCLUDED.modelo_veiculo,
                    local_apresentacao = EXCLUDED.local_apresentacao,
                    hora_inicio        = EXCLUDED.hora_inicio,
                    hora_fim           = EXCLUDED.hora_fim,
                    updated_at         = NOW()
                RETURNING (xmax = 0) AS inserido
            """, (
                mot_id, contrato, drv.get("clientName"),
                turno, normalizar_tipo(drv.get("type", "Fixo")),
                veiculo.get("prefixo"), veiculo.get("plate"), modelo,
                loc.get("name"),
                shift.get("startTime"), shift.get("endTime"),
            ))
            if cur.fetchone()[0]: esc_ins += 1
            else:                 esc_upd += 1

        # ── Férias ─────────────────────────────────────────────────────────
        for vac in item.get("vacations", []):
            sub_id = _resolve_substituto(cur, vac.get("substituteDriver"))
            cur.execute("""
                INSERT INTO ferias (motorista_id, api_id, data_inicio, data_fim,
                                    substituto_id, publish_date)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (api_id) DO UPDATE SET
                    data_inicio   = EXCLUDED.data_inicio,
                    data_fim      = EXCLUDED.data_fim,
                    substituto_id = EXCLUDED.substituto_id,
                    publish_date  = EXCLUDED.publish_date,
                    updated_at    = NOW()
                RETURNING (xmax = 0) AS inserido
            """, (
                mot_id, vac["id"],
                vac["startDate"], vac["endDate"],
                sub_id, vac.get("publishDate"),
            ))
            if cur.fetchone()[0]: fer_ins += 1
            else:                 fer_upd += 1

        # ── Folgas ─────────────────────────────────────────────────────────
        for off in item.get("dayOffs", []):
            sub_id = _resolve_substituto(cur, off.get("substituteDriver"))
            cur.execute("""
                INSERT INTO folga (motorista_id, api_id, data_folga,
                                   substituto_id, publish_date)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (api_id) DO UPDATE SET
                    data_folga    = EXCLUDED.data_folga,
                    substituto_id = EXCLUDED.substituto_id,
                    publish_date  = EXCLUDED.publish_date,
                    updated_at    = NOW()
                RETURNING (xmax = 0) AS inserido
            """, (
                mot_id, off["id"], off["date"],
                sub_id, off.get("publishDate"),
            ))
            if cur.fetchone()[0]: fol_ins += 1
            else:                 fol_upd += 1

    log(f"  motoristas → {mot_ins} inseridos / {mot_upd} atualizados")
    log(f"  escala     → {esc_ins} inseridos / {esc_upd} atualizados")
    log(f"  férias     → {fer_ins} inseridos / {fer_upd} atualizados")
    log(f"  folgas     → {fol_ins} inseridos / {fol_upd} atualizados")


def _resolve_substituto(cur, substituto):
    """Retorna o id interno do motorista substituto, ou None."""
    if not substituto:
        return None
    cur.execute("SELECT id FROM motoristas WHERE chapa = %s",
                (str(substituto["employeeId"]),))
    row = cur.fetchone()
    return row[0] if row else None


# ──────────────────────────────────────────────────────────────────────────────
# ETAPA 3 — SYNC LARGADAS
# ──────────────────────────────────────────────────────────────────────────────

def sync_largadas(cur):
    separador("=")
    print("  ETAPA 3 — Sync largadas")
    separador("=")

    since = (datetime.now(BRT) - timedelta(days=SYNC_DAYS)).strftime("%Y-%m-%dT00:00:00")
    log(f"🌐 Buscando largadas desde {since}...")

    registros = api_get("last-departures-by-date", params={"date": since})
    log(f"   {len(registros)} largadas recebidas\n")

    inseridos = atualizados = 0

    for r in registros:
        cur.execute("""
            INSERT INTO largadas (
                departure_id, shift_name, driver_name, plate, prefixo,
                vehicle_model, number_contract, supplier_customer, trade_name,
                local_apresentacao, start_time, end_time, departure_datetime,
                synced_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
            ON CONFLICT (departure_id) DO UPDATE SET
                shift_name         = EXCLUDED.shift_name,
                driver_name        = EXCLUDED.driver_name,
                plate              = EXCLUDED.plate,
                prefixo            = EXCLUDED.prefixo,
                vehicle_model      = EXCLUDED.vehicle_model,
                number_contract    = EXCLUDED.number_contract,
                supplier_customer  = EXCLUDED.supplier_customer,
                trade_name         = EXCLUDED.trade_name,
                local_apresentacao = EXCLUDED.local_apresentacao,
                start_time         = EXCLUDED.start_time,
                end_time           = EXCLUDED.end_time,
                departure_datetime = EXCLUDED.departure_datetime,
                synced_at          = NOW()
            RETURNING (xmax = 0) AS inserido
        """, (
            r["departureId"],      r.get("shiftName"),   r.get("driverName"),
            r.get("plate"),        r.get("prefixo"),     r.get("vehicleModel"),
            r.get("numberContract"), r.get("supplierCustomer"), r.get("tradeName"),
            r.get("name"),         r.get("startTime"),   r.get("endTime"),
            r.get("departureDateTime"),
        ))
        if cur.fetchone()[0]: inseridos   += 1
        else:                 atualizados += 1

    log(f"  largadas → {inseridos} inseridas / {atualizados} atualizadas")


# ──────────────────────────────────────────────────────────────────────────────
# RELATÓRIO FINAL
# ──────────────────────────────────────────────────────────────────────────────

def relatorio(cur):
    tabelas = ["motoristas", "escala", "largadas", "ferias", "folga"]
    separador()
    print(f"  {'Tabela':<18} {'Registros':>10}")
    separador()
    for t in tabelas:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        qtd = cur.fetchone()[0]
        print(f"  {t:<18} {qtd:>10}")
    separador()


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*55}")
    print(f"  Caravan Supabase — {datetime.now(BRT).strftime('%Y-%m-%d %H:%M:%S')} BRT")
    print(f"{'='*55}\n")

    conn = get_connection()
    conn.autocommit = False
    cur  = conn.cursor()

    try:
        # 1. DDL (nunca aborta — erros viram avisos com savepoint)
        criar_tabelas(cur)
        conn.commit()

        # 2. Motoristas / Escala / Férias / Folgas
        sync_drivers(cur)
        conn.commit()

        # 3. Largadas
        sync_largadas(cur)
        conn.commit()

        print()
        relatorio(cur)
        print("\n  ✅ Concluído\n")

    except Exception as e:
        conn.rollback()
        print(f"\n  ❌ Erro inesperado: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()