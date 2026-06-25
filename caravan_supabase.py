"""
caravan_supabase.py
Cria as tabelas do Caravan no Supabase e sincroniza os dados das APIs.

Uso local:
    export $(cat .env | xargs)
    python caravan_supabase.py

Via Render + UptimeRobot:
    O servidor Flask em app.py agenda a execução às 7h e 17h (BRT).
    O UptimeRobot monitora GET / a cada 5 min para manter o serviço ativo.
"""

import os
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
SYNC_DAYS = 30
# Fração mínima de assigned (motoristas com escala) que o feed precisa trazer, em
# relação aos que o banco já conhece, para liberar a reclassificação dos ausentes
# para NENHUM. Protege contra feed curto por soluço da API (ver sync_drivers).
RECLASS_MIN_RATIO = 0.9


def get_connection():
    config = {
        "host":            os.environ.get("SUPABASE_HOST"),
        "port":            os.environ.get("SUPABASE_PORTA", "5432"),
        "dbname":          os.environ.get("SUPABASE_BANCO", "postgres"),
        "user":            os.environ.get("SUPABASE_USUARIO"),
        "password":        os.environ.get("SUPABASE_SENHA"),
        "sslmode":         "require",
        "connect_timeout": 10,
        "options":         "-c timezone=America/Sao_Paulo",
    }
    obrigatorias = {
        "SUPABASE_HOST":    config["host"],
        "SUPABASE_USUARIO": config["user"],
        "SUPABASE_SENHA":   config["password"],
        "CARAVAN_API_KEY":  API_KEY,
    }
    faltando = [k for k, v in obrigatorias.items() if not v]
    if faltando:
        for var in faltando:
            print(f"  ❌ Variável não encontrada: {var}")
        raise RuntimeError(f"Variáveis de ambiente obrigatórias não configuradas: {', '.join(faltando)}")
    return psycopg2.connect(**config)


# ──────────────────────────────────────────────────────────────────────────────
# DDL
# Cada item é uma única statement. Savepoints isolam falhas — um erro
# vira ⚠️ aviso e o script continua (útil em re-execuções).
# ──────────────────────────────────────────────────────────────────────────────

STATEMENTS = [

    # ── FUSO HORÁRIO ──────────────────────────────────────────────────────────
    {"label": "fuso horário: America/Sao_Paulo", "sql":
        "ALTER DATABASE postgres SET timezone TO 'America/Sao_Paulo'"},

    # ── MOTORISTAS ────────────────────────────────────────────────────────────
    {"label": "tabela: motoristas", "sql": """
        CREATE TABLE IF NOT EXISTS motoristas (
            id          SERIAL      PRIMARY KEY,
            chapa       VARCHAR(20) NOT NULL UNIQUE,
            nome        VARCHAR(200) NOT NULL,
            funcao      VARCHAR(100),
            status      VARCHAR(20) NOT NULL DEFAULT 'ATIVO'
                            CHECK (status IN ('ATIVO','INATIVO','AFASTADO')),
            tipo_escala VARCHAR(20)
                            CHECK (tipo_escala IN (
                                'FIXO','FOLGUISTA','RESERVA','AFASTADO','APOIO','FERISTA','NENHUM'
                            )),
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """},

    # Garante a coluna em bancos já existentes (CREATE TABLE IF NOT EXISTS não altera tabela pronta).
    # O campo 'type' da API (Fixo/Afastado/Folguista/Ferista/...) carrega a situação real do motorista;
    # 'status' da API vem sempre como "Ativo", por isso não diferencia nada.
    {"label": "coluna: motoristas.tipo_escala", "sql": """
        ALTER TABLE motoristas
            ADD COLUMN IF NOT EXISTS tipo_escala VARCHAR(20)
    """},
    # Recria o CHECK incluindo 'NENHUM' (migra bancos cujo constraint não tinha o valor).
    {"label": "constraint: motoristas.tipo_escala drop", "sql":
        "ALTER TABLE motoristas DROP CONSTRAINT IF EXISTS motoristas_tipo_escala_check"},
    {"label": "constraint: motoristas.tipo_escala (+NENHUM)", "sql": """
        ALTER TABLE motoristas ADD CONSTRAINT motoristas_tipo_escala_check
            CHECK (tipo_escala IN (
                'FIXO','FOLGUISTA','RESERVA','AFASTADO','APOIO','FERISTA','NENHUM'
            ))
    """},

    # ── ESCALA ────────────────────────────────────────────────────────────────
    {"label": "tabela: escala", "sql": """
        CREATE TABLE IF NOT EXISTS escala (
            id                 SERIAL      PRIMARY KEY,
            motorista_id       INTEGER     NOT NULL
                                   REFERENCES motoristas(id) ON DELETE CASCADE,
            numero_contrato    VARCHAR(20) NOT NULL,
            cliente            VARCHAR(200),
            turno              VARCHAR(30) NOT NULL
                                   CHECK (turno IN (
                                       'PRIMEIRO TURNO','SEGUNDO TURNO','TERCEIRO TURNO'
                                   )),
            tipo_escala        VARCHAR(20) NOT NULL DEFAULT 'FIXO'
                                   CHECK (tipo_escala IN (
                                       'FIXO','FOLGUISTA','RESERVA','AFASTADO','APOIO','FERISTA','NENHUM'
                                   )),
            prefixo            VARCHAR(20),
            placa              VARCHAR(20),
            modelo_veiculo     VARCHAR(100),
            local_apresentacao VARCHAR(200),
            hora_inicio        TIME,
            hora_fim           TIME,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT escala_motorista_contrato_unica
                UNIQUE (motorista_id, numero_contrato)
        )
    """},
    # Recria o CHECK incluindo 'NENHUM' (migra bancos cujo constraint não tinha o valor).
    {"label": "constraint: escala.tipo_escala drop", "sql":
        "ALTER TABLE escala DROP CONSTRAINT IF EXISTS escala_tipo_escala_check"},
    {"label": "constraint: escala.tipo_escala (+NENHUM)", "sql": """
        ALTER TABLE escala ADD CONSTRAINT escala_tipo_escala_check
            CHECK (tipo_escala IN (
                'FIXO','FOLGUISTA','RESERVA','AFASTADO','APOIO','FERISTA','NENHUM'
            ))
    """},

    # ── LARGADAS ──────────────────────────────────────────────────────────────
    {"label": "tabela: largadas", "sql": """
        CREATE TABLE IF NOT EXISTS largadas (
            id                 SERIAL      PRIMARY KEY,
            departure_id       INTEGER     UNIQUE,
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
            synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """},

    # ── FERIAS ────────────────────────────────────────────────────────────────
    {"label": "tabela: ferias", "sql": """
        CREATE TABLE IF NOT EXISTS ferias (
            id            SERIAL      PRIMARY KEY,
            motorista_id  INTEGER     NOT NULL
                              REFERENCES motoristas(id) ON DELETE CASCADE,
            api_id        INTEGER     UNIQUE,
            data_inicio   DATE        NOT NULL,
            data_fim      DATE        NOT NULL,
            dias_gozados  INTEGER     GENERATED ALWAYS AS (
                              (data_fim - data_inicio + 1)
                          ) STORED,
            substituto_id INTEGER     REFERENCES motoristas(id) ON DELETE SET NULL,
            publish_date  DATE,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT ferias_datas_validas CHECK (data_fim >= data_inicio)
        )
    """},

    # ── FOLGA ─────────────────────────────────────────────────────────────────
    {"label": "tabela: folga", "sql": """
        CREATE TABLE IF NOT EXISTS folga (
            id            SERIAL      PRIMARY KEY,
            motorista_id  INTEGER     NOT NULL
                              REFERENCES motoristas(id) ON DELETE CASCADE,
            api_id        INTEGER     UNIQUE,
            data_folga    DATE        NOT NULL,
            substituto_id INTEGER     REFERENCES motoristas(id) ON DELETE SET NULL,
            publish_date  DATE,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT folga_unica_por_dia UNIQUE (motorista_id, data_folga)
        )
    """},

    # ── ÍNDICES ───────────────────────────────────────────────────────────────
    {"label": "índice: motoristas.chapa",    "sql": "CREATE INDEX IF NOT EXISTS idx_motoristas_chapa  ON motoristas(chapa)"},
    {"label": "índice: motoristas.nome",     "sql": "CREATE INDEX IF NOT EXISTS idx_motoristas_nome   ON motoristas(nome)"},
    {"label": "índice: escala.motorista_id", "sql": "CREATE INDEX IF NOT EXISTS idx_escala_motorista  ON escala(motorista_id)"},
    {"label": "índice: escala.contrato",     "sql": "CREATE INDEX IF NOT EXISTS idx_escala_contrato   ON escala(numero_contrato)"},
    {"label": "índice: largadas.datetime",   "sql": "CREATE INDEX IF NOT EXISTS idx_largadas_dt       ON largadas(departure_datetime DESC)"},
    {"label": "índice: largadas.contrato",   "sql": "CREATE INDEX IF NOT EXISTS idx_largadas_contrato ON largadas(number_contract)"},
    {"label": "índice: ferias.motorista_id", "sql": "CREATE INDEX IF NOT EXISTS idx_ferias_motorista  ON ferias(motorista_id)"},
    {"label": "índice: ferias.datas",        "sql": "CREATE INDEX IF NOT EXISTS idx_ferias_datas      ON ferias(data_inicio, data_fim)"},
    {"label": "índice: folga.motorista_id",  "sql": "CREATE INDEX IF NOT EXISTS idx_folga_motorista   ON folga(motorista_id)"},
    {"label": "índice: folga.data",          "sql": "CREATE INDEX IF NOT EXISTS idx_folga_data        ON folga(data_folga)"},

    # ── TRIGGER updated_at ────────────────────────────────────────────────────
    {"label": "função: set_updated_at", "sql": """
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
        $$ LANGUAGE plpgsql
    """},
    {"label": "trigger: motoristas", "sql": """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_motoristas_updated_at') THEN
                CREATE TRIGGER trg_motoristas_updated_at
                BEFORE UPDATE ON motoristas
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
            END IF;
        END $$
    """},
    {"label": "trigger: escala", "sql": """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_escala_updated_at') THEN
                CREATE TRIGGER trg_escala_updated_at
                BEFORE UPDATE ON escala
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
            END IF;
        END $$
    """},
    {"label": "trigger: ferias", "sql": """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_ferias_updated_at') THEN
                CREATE TRIGGER trg_ferias_updated_at
                BEFORE UPDATE ON ferias
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
            END IF;
        END $$
    """},
    {"label": "trigger: folga", "sql": """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_folga_updated_at') THEN
                CREATE TRIGGER trg_folga_updated_at
                BEFORE UPDATE ON folga
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

def sep(char="─", n=55):
    print(char * n)

def api_get(path, params=None):
    resp = requests.get(f"{API_BASE}/{path}", headers=HEADERS,
                        params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def norm_turno(s):
    s = (s or "").lower()
    if "primeiro" in s or "1" in s: return "PRIMEIRO TURNO"
    if "segundo"  in s or "2" in s: return "SEGUNDO TURNO"
    if "terceiro" in s or "3" in s: return "TERCEIRO TURNO"
    return "PRIMEIRO TURNO"

def norm_tipo(s):
    # 'Nenhum' = motorista sem escala atribuída. A API só expõe um punhado
    # flutuante deles (a maioria dos ~300 nunca vem no feed), então NÃO entram
    # no FIXO — vão para o balde próprio NENHUM, que é ignorado nas consultas.
    return {"fixo":"FIXO","folguista":"FOLGUISTA","reserva":"RESERVA",
            "afastado":"AFASTADO","apoio":"APOIO","ferista":"FERISTA",
            "nenhum":"NENHUM"}.get((s or "").lower(), "FIXO")

def norm_status(s):
    s = (s or "").lower()
    if "ativo"   in s: return "ATIVO"
    if "inativo" in s: return "INATIVO"
    return "AFASTADO"

def resolve_sub(cur, sub):
    if not sub: return None
    cur.execute("SELECT id FROM motoristas WHERE chapa = %s", (str(sub["employeeId"]),))
    row = cur.fetchone()
    return row[0] if row else None


# ──────────────────────────────────────────────────────────────────────────────
# ETAPA 1 — DDL
# ──────────────────────────────────────────────────────────────────────────────

def criar_tabelas(cur):
    sep("=")
    print("  ETAPA 1 — Estrutura das tabelas")
    sep("=")
    avisos = 0
    for i, item in enumerate(STATEMENTS, 1):
        try:
            cur.execute("SAVEPOINT sp")
            cur.execute(item["sql"])
            cur.execute("RELEASE SAVEPOINT sp")
            log(f"✅ [{i:02d}] {item['label']}")
        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT sp")
            log(f"⚠️  [{i:02d}] {item['label']} — {str(e).splitlines()[0]}")
            avisos += 1
    if avisos:
        log(f"\n  ℹ️  {avisos} aviso(s) — objetos já existentes (normal em re-execução)")


# ──────────────────────────────────────────────────────────────────────────────
# ETAPA 2 — SYNC MOTORISTAS / ESCALA / FÉRIAS / FOLGAS
# ──────────────────────────────────────────────────────────────────────────────

def sync_drivers(cur):
    sep("=")
    print("  ETAPA 2 — Sync motoristas / escala / férias / folgas")
    sep("=")
    log("🌐 Buscando driver-consolidated...")
    dados = api_get("driver-consolidated")
    log(f"   {len(dados)} motoristas recebidos\n")

    mi = mu = ei = eu = fi = fu = oi = ou = 0
    chapas_vistas = []
    assigned_vistos = 0   # motoristas com escala (tipo != NENHUM) vindos no feed

    for item in dados:
        drv   = item["driver"]
        chapa = str(drv["employeeId"])
        tipo  = norm_tipo(drv.get("type","Fixo"))
        chapas_vistas.append(chapa)
        if tipo != "NENHUM": assigned_vistos += 1

        # Motorista
        cur.execute("""
            INSERT INTO motoristas (chapa, nome, funcao, status, tipo_escala)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (chapa) DO UPDATE SET
                nome=EXCLUDED.nome, funcao=EXCLUDED.funcao,
                status=EXCLUDED.status, tipo_escala=EXCLUDED.tipo_escala,
                updated_at=NOW()
            RETURNING (xmax=0)
        """, (chapa, drv["name"], drv.get("position"),
              norm_status(drv.get("status","Ativo")), tipo))
        if cur.fetchone()[0]: mi += 1
        else: mu += 1

        cur.execute("SELECT id FROM motoristas WHERE chapa=%s", (chapa,))
        mot_id = cur.fetchone()[0]

        # Escala — só registra se houver contrato. A API às vezes devolve um
        # motorista 'Nenhum' com regularSchedule "fantasma" e contractNumber null;
        # como numero_contrato é NOT NULL + parte da UNIQUE(motorista_id, contrato),
        # escala sem contrato nunca foi registro válido — pulamos (sem perder dado:
        # escala legítima sempre traz contrato).
        sched    = item.get("regularSchedule") or {}
        contrato = (drv.get("contractNumber") or "").strip()
        if sched and contrato:
            v      = sched.get("vehicle") or {}
            shift  = sched.get("shift") or drv.get("shift") or {}
            loc    = sched.get("startLocation") or {}
            modelo = " - ".join(filter(None,[v.get("brandName"),v.get("modelName")])) or None
            cur.execute("""
                INSERT INTO escala (
                    motorista_id, numero_contrato, cliente, turno, tipo_escala,
                    prefixo, placa, modelo_veiculo, local_apresentacao,
                    hora_inicio, hora_fim
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (motorista_id, numero_contrato) DO UPDATE SET
                    cliente=EXCLUDED.cliente, turno=EXCLUDED.turno,
                    tipo_escala=EXCLUDED.tipo_escala, prefixo=EXCLUDED.prefixo,
                    placa=EXCLUDED.placa, modelo_veiculo=EXCLUDED.modelo_veiculo,
                    local_apresentacao=EXCLUDED.local_apresentacao,
                    hora_inicio=EXCLUDED.hora_inicio, hora_fim=EXCLUDED.hora_fim,
                    updated_at=NOW()
                RETURNING (xmax=0)
            """, (mot_id, contrato, drv.get("clientName"),
                  norm_turno(shift.get("shiftName","")), tipo,
                  v.get("prefixo"), v.get("plate"), modelo, loc.get("name"),
                  shift.get("startTime"), shift.get("endTime")))
            if cur.fetchone()[0]: ei += 1
            else: eu += 1

        # Férias
        for vac in item.get("vacations", []):
            cur.execute("""
                INSERT INTO ferias (motorista_id, api_id, data_inicio, data_fim,
                                    substituto_id, publish_date)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (api_id) DO UPDATE SET
                    data_inicio=EXCLUDED.data_inicio, data_fim=EXCLUDED.data_fim,
                    substituto_id=EXCLUDED.substituto_id,
                    publish_date=EXCLUDED.publish_date, updated_at=NOW()
                RETURNING (xmax=0)
            """, (mot_id, vac["id"], vac["startDate"], vac["endDate"],
                  resolve_sub(cur, vac.get("substituteDriver")),
                  vac.get("publishDate")))
            if cur.fetchone()[0]: fi += 1
            else: fu += 1

        # Folgas
        for off in item.get("dayOffs", []):
            cur.execute("""
                INSERT INTO folga (motorista_id, api_id, data_folga,
                                   substituto_id, publish_date)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (motorista_id, data_folga) DO UPDATE SET
                    api_id=EXCLUDED.api_id,
                    substituto_id=EXCLUDED.substituto_id,
                    publish_date=EXCLUDED.publish_date, updated_at=NOW()
                RETURNING (xmax=0)
            """, (mot_id, off["id"], off["date"],
                  resolve_sub(cur, off.get("substituteDriver")),
                  off.get("publishDate")))
            if cur.fetchone()[0]: oi += 1
            else: ou += 1

    # Quem NÃO veio no feed = motorista sem escala atribuída ('Nenhum'). A API só
    # expõe os assigned (Fixo/Folguista/Reserva/Afastado/Apoio/Ferista — estáveis
    # entre chamadas) + um punhado flutuante de 'Nenhum'; os ~300 'Nenhum' restantes
    # nunca vêm. Por isso "ausente do feed" ⇔ "Nenhum", e marcamos tipo_escala=NENHUM
    # (não INATIVO: ausência não é demissão, a API não tem sinal de desligamento).
    # Isso também limpa os 'Nenhum' que ficaram presos como FIXO de feeds antigos,
    # fazendo os tipos assigned baterem com o sistema. status volta a ATIVO para
    # desfazer marcações INATIVO antigas. Nada deleta — escala/férias/folgas ficam.
    #
    # SALVAGUARDA: a API às vezes engasga e devolve um feed curto. Se varrêssemos
    # cegamente, um soluço transformaria assigned reais em NENHUM (perderíamos gente
    # da contagem). Só reclassificamos se o nº de assigned do feed estiver dentro de
    # RECLASS_MIN_RATIO do que o banco já conhece; senão pulamos e avisamos.
    cur.execute("SELECT COUNT(*) FROM motoristas WHERE tipo_escala IS DISTINCT FROM 'NENHUM'")
    assigned_no_banco = cur.fetchone()[0]
    piso = assigned_no_banco * RECLASS_MIN_RATIO

    if assigned_vistos >= piso:
        cur.execute("""
            UPDATE motoristas SET tipo_escala='NENHUM', status='ATIVO', updated_at=NOW()
            WHERE chapa <> ALL(%s)
              AND (tipo_escala IS DISTINCT FROM 'NENHUM' OR status <> 'ATIVO')
        """, (chapas_vistas,))
        nenhum = cur.rowcount
        log(f"  motoristas → {mi} inseridos / {mu} atualizados / {nenhum} fora do feed → NENHUM")
    else:
        log(f"  motoristas → {mi} inseridos / {mu} atualizados")
        log(f"  ⚠️  feed curto: {assigned_vistos} assigned vindos < piso {piso:.0f} "
            f"({assigned_no_banco} no banco × {RECLASS_MIN_RATIO}). "
            f"Reclassificação p/ NENHUM PULADA — possível soluço da API.")
    log(f"  escala     → {ei} inseridos / {eu} atualizados")
    log(f"  férias     → {fi} inseridos / {fu} atualizados")
    log(f"  folgas     → {oi} inseridos / {ou} atualizados")


# ──────────────────────────────────────────────────────────────────────────────
# ETAPA 3 — SYNC LARGADAS
# ──────────────────────────────────────────────────────────────────────────────

def sync_largadas(cur):
    sep("=")
    print("  ETAPA 3 — Sync largadas")
    sep("=")
    since = (datetime.now(BRT) - timedelta(days=SYNC_DAYS)).strftime("%Y-%m-%dT00:00:00")
    log(f"🌐 Buscando largadas desde {since}...")
    registros = api_get("last-departures-by-date", params={"date": since})
    log(f"   {len(registros)} largadas recebidas\n")

    ins = upd = 0
    for r in registros:
        cur.execute("""
            INSERT INTO largadas (
                departure_id, shift_name, driver_name, plate, prefixo,
                vehicle_model, number_contract, supplier_customer, trade_name,
                local_apresentacao, start_time, end_time, departure_datetime, synced_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (departure_id) DO UPDATE SET
                shift_name=EXCLUDED.shift_name, driver_name=EXCLUDED.driver_name,
                plate=EXCLUDED.plate, prefixo=EXCLUDED.prefixo,
                vehicle_model=EXCLUDED.vehicle_model,
                number_contract=EXCLUDED.number_contract,
                supplier_customer=EXCLUDED.supplier_customer,
                trade_name=EXCLUDED.trade_name,
                local_apresentacao=EXCLUDED.local_apresentacao,
                start_time=EXCLUDED.start_time, end_time=EXCLUDED.end_time,
                departure_datetime=EXCLUDED.departure_datetime, synced_at=NOW()
            RETURNING (xmax=0)
        """, (r["departureId"], r.get("shiftName"), r.get("driverName"),
              r.get("plate"), r.get("prefixo"), r.get("vehicleModel"),
              r.get("numberContract"), r.get("supplierCustomer"), r.get("tradeName"),
              r.get("name"), r.get("startTime"), r.get("endTime"),
              r.get("departureDateTime")))
        if cur.fetchone()[0]: ins += 1
        else: upd += 1

    log(f"  largadas → {ins} inseridas / {upd} atualizadas")


# ──────────────────────────────────────────────────────────────────────────────
# RELATÓRIO FINAL
# ──────────────────────────────────────────────────────────────────────────────

def relatorio(cur):
    sep()
    print(f"  {'Tabela':<18} {'Registros':>10}")
    sep()
    for t in ["motoristas","escala","largadas","ferias","folga"]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t:<18} {cur.fetchone()[0]:>10}")
    sep()


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
        criar_tabelas(cur)
        conn.commit()

        sync_drivers(cur)
        conn.commit()

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