"""
caravan_supabase.py
Cria as tabelas do Caravan no Supabase (PostgreSQL).

Uso local:
    export $(cat .env | xargs)
    python caravan_supabase.py

Via GitHub Actions:
    Disparado pelo workflow .github/workflows/main.yml
"""

import os
import sys
import psycopg2
from datetime import datetime


# ──────────────────────────────────────────────────────────────────────────────
# CONEXÃO
# ──────────────────────────────────────────────────────────────────────────────

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
    }

    faltando = [var for var, val in obrigatorias.items() if not val]
    if faltando:
        for var in faltando:
            print(f"❌ Variável não encontrada: {var}")
        sys.exit(1)

    return psycopg2.connect(**config)


# ──────────────────────────────────────────────────────────────────────────────
# DDL — TABELAS, ÍNDICES E TRIGGERS
# ──────────────────────────────────────────────────────────────────────────────

STATEMENTS = [

    # ── MOTORISTAS ────────────────────────────────────────────────────────────
    # Cadastro base de todos os motoristas da frota.
    # Fonte: relatório "Relação - Motoristas por Contrato" (Chapa / Nome / Função)
    {
        "label": "tabela: motoristas",
        "sql": """
            CREATE TABLE IF NOT EXISTS motoristas (
                id          SERIAL       PRIMARY KEY,
                chapa       VARCHAR(20)  NOT NULL UNIQUE,
                nome        VARCHAR(200) NOT NULL,
                funcao      VARCHAR(100),
                status      VARCHAR(20)  NOT NULL DEFAULT 'ATIVO'
                                CHECK (status IN ('ATIVO', 'INATIVO', 'AFASTADO')),
                created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            );
            COMMENT ON TABLE  motoristas        IS 'Cadastro de motoristas da frota Caravan';
            COMMENT ON COLUMN motoristas.chapa  IS 'Número de chapa funcional único';
            COMMENT ON COLUMN motoristas.funcao IS 'Ex: MOTORISTA/CAMINHAO, MOTORISTA/VEICULOS LEVES, MOTORISTA DE ONIBUS';
            COMMENT ON COLUMN motoristas.status IS 'ATIVO | INATIVO | AFASTADO';
        """,
    },

    # ── ESCALA ────────────────────────────────────────────────────────────────
    # Lotação do motorista por contrato e turno.
    # tipo_escala espelha os grupos do relatório: Fixo / Folguista / Reserva / Afastado
    {
        "label": "tabela: escala",
        "sql": """
            CREATE TABLE IF NOT EXISTS escala (
                id                 SERIAL       PRIMARY KEY,
                motorista_id       INTEGER      NOT NULL
                                       REFERENCES motoristas(id) ON DELETE CASCADE,
                numero_contrato    VARCHAR(20)  NOT NULL,
                cliente            VARCHAR(200),
                turno              VARCHAR(30)  NOT NULL
                                       CHECK (turno IN (
                                           'PRIMEIRO TURNO', 'SEGUNDO TURNO', 'TERCEIRO TURNO'
                                       )),
                tipo_escala        VARCHAR(20)  NOT NULL DEFAULT 'FIXO'
                                       CHECK (tipo_escala IN (
                                           'FIXO', 'FOLGUISTA', 'RESERVA', 'AFASTADO'
                                       )),
                prefixo            VARCHAR(20),
                modelo_veiculo     VARCHAR(100),
                local_apresentacao VARCHAR(200),
                data_inicio        DATE         NOT NULL,
                data_fim           DATE,
                hora_inicio        TIME,
                hora_fim           TIME,
                observacao         TEXT,
                created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            );
            COMMENT ON TABLE  escala                    IS 'Escala de turnos dos motoristas por contrato';
            COMMENT ON COLUMN escala.numero_contrato    IS 'Ex: 001/2026, 003/2026, 006/2020, 008/2025';
            COMMENT ON COLUMN escala.tipo_escala        IS 'FIXO | FOLGUISTA | RESERVA | AFASTADO';
            COMMENT ON COLUMN escala.prefixo            IS 'Prefixo do veículo (ex: 5781.0, 3001.6)';
            COMMENT ON COLUMN escala.local_apresentacao IS 'Ex: MAAS-SEINFRA, 7° P. A. VILA AURORA';
        """,
    },

    # ── LARGADAS ──────────────────────────────────────────────────────────────
    # Registros sincronizados via API:
    # GET /integration/last-departures-by-date?date=YYYY-MM-DDTHH:MM:SS
    # Campos mapeados 1-para-1 com o payload JSON da API.
    # departure_id é a PK externa — garante idempotência na sincronização.
    {
        "label": "tabela: largadas",
        "sql": """
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
            );
            COMMENT ON TABLE  largadas                    IS 'Largadas obtidas via API /integration/last-departures-by-date';
            COMMENT ON COLUMN largadas.departure_id       IS 'departureId da API — chave de idempotência';
            COMMENT ON COLUMN largadas.departure_datetime IS 'Data e hora exata da largada';
            COMMENT ON COLUMN largadas.synced_at          IS 'Timestamp da última sincronização com a API';
        """,
    },

    # ── FERIAS ────────────────────────────────────────────────────────────────
    # Períodos de férias por motorista.
    # dias_gozados é coluna gerada — calculada automaticamente pelo banco.
    {
        "label": "tabela: ferias",
        "sql": """
            CREATE TABLE IF NOT EXISTS ferias (
                id           SERIAL       PRIMARY KEY,
                motorista_id INTEGER      NOT NULL
                                 REFERENCES motoristas(id) ON DELETE CASCADE,
                data_inicio  DATE         NOT NULL,
                data_fim     DATE         NOT NULL,
                dias_gozados INTEGER      GENERATED ALWAYS AS (
                                 (data_fim - data_inicio + 1)
                             ) STORED,
                periodo_ref  VARCHAR(20),
                aprovado_por VARCHAR(200),
                observacao   TEXT,
                created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                CONSTRAINT ferias_datas_validas CHECK (data_fim >= data_inicio)
            );
            COMMENT ON TABLE  ferias              IS 'Períodos de férias dos motoristas';
            COMMENT ON COLUMN ferias.dias_gozados IS 'Calculado automaticamente: data_fim - data_inicio + 1';
            COMMENT ON COLUMN ferias.periodo_ref  IS 'Período aquisitivo de referência (ex: 2025/2026)';
        """,
    },

    # ── FOLGA ─────────────────────────────────────────────────────────────────
    # Registro diário de ausências (folgas, faltas, abonos).
    # substituto_id → motorista Folguista/Reserva que cobriu a ausência.
    # UNIQUE em (motorista_id, data_folga, turno_afetado) evita duplicatas.
    {
        "label": "tabela: folga",
        "sql": """
            CREATE TABLE IF NOT EXISTS folga (
                id            SERIAL       PRIMARY KEY,
                motorista_id  INTEGER      NOT NULL
                                  REFERENCES motoristas(id) ON DELETE CASCADE,
                data_folga    DATE         NOT NULL,
                tipo          VARCHAR(30)  NOT NULL DEFAULT 'FOLGA'
                                  CHECK (tipo IN (
                                      'FOLGA', 'FOLGA_COMPENSATORIA', 'ABONO',
                                      'FALTA_JUSTIFICADA', 'FALTA_INJUSTIFICADA'
                                  )),
                turno_afetado VARCHAR(30)
                                  CHECK (turno_afetado IN (
                                      'PRIMEIRO TURNO', 'SEGUNDO TURNO',
                                      'TERCEIRO TURNO', 'DIA_INTEIRO'
                                  )),
                substituto_id INTEGER      REFERENCES motoristas(id) ON DELETE SET NULL,
                observacao    TEXT,
                created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                CONSTRAINT folga_unica_por_dia
                    UNIQUE (motorista_id, data_folga, turno_afetado)
            );
            COMMENT ON TABLE  folga               IS 'Folgas, faltas e ausências dos motoristas';
            COMMENT ON COLUMN folga.tipo          IS 'Tipo da ausência';
            COMMENT ON COLUMN folga.substituto_id IS 'Motorista Folguista/Reserva que cobriu a ausência';
        """,
    },

    # ── ÍNDICES ───────────────────────────────────────────────────────────────
    {"label": "índice: motoristas.chapa",       "sql": "CREATE INDEX IF NOT EXISTS idx_motoristas_chapa    ON motoristas(chapa);"},
    {"label": "índice: motoristas.nome",        "sql": "CREATE INDEX IF NOT EXISTS idx_motoristas_nome     ON motoristas(nome);"},
    {"label": "índice: escala.motorista_id",    "sql": "CREATE INDEX IF NOT EXISTS idx_escala_motorista    ON escala(motorista_id);"},
    {"label": "índice: escala.numero_contrato", "sql": "CREATE INDEX IF NOT EXISTS idx_escala_contrato     ON escala(numero_contrato);"},
    {"label": "índice: escala.datas",           "sql": "CREATE INDEX IF NOT EXISTS idx_escala_datas        ON escala(data_inicio, data_fim);"},
    {"label": "índice: largadas.datetime",      "sql": "CREATE INDEX IF NOT EXISTS idx_largadas_dt         ON largadas(departure_datetime DESC);"},
    {"label": "índice: largadas.contrato",      "sql": "CREATE INDEX IF NOT EXISTS idx_largadas_contrato   ON largadas(number_contract);"},
    {"label": "índice: ferias.motorista_id",    "sql": "CREATE INDEX IF NOT EXISTS idx_ferias_motorista    ON ferias(motorista_id);"},
    {"label": "índice: folga.motorista_id",     "sql": "CREATE INDEX IF NOT EXISTS idx_folga_motorista     ON folga(motorista_id);"},
    {"label": "índice: folga.data_folga",       "sql": "CREATE INDEX IF NOT EXISTS idx_folga_data          ON folga(data_folga);"},

    # ── TRIGGER: atualiza updated_at automaticamente ──────────────────────────
    {
        "label": "função: set_updated_at",
        "sql": """
            CREATE OR REPLACE FUNCTION set_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """,
    },
    {
        "label": "trigger: motoristas.updated_at",
        "sql": """
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_motoristas_updated_at') THEN
                    CREATE TRIGGER trg_motoristas_updated_at
                    BEFORE UPDATE ON motoristas
                    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
                END IF;
            END $$;
        """,
    },
    {
        "label": "trigger: escala.updated_at",
        "sql": """
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_escala_updated_at') THEN
                    CREATE TRIGGER trg_escala_updated_at
                    BEFORE UPDATE ON escala
                    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
                END IF;
            END $$;
        """,
    },
    {
        "label": "trigger: ferias.updated_at",
        "sql": """
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_ferias_updated_at') THEN
                    CREATE TRIGGER trg_ferias_updated_at
                    BEFORE UPDATE ON ferias
                    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
                END IF;
            END $$;
        """,
    },
    {
        "label": "trigger: folga.updated_at",
        "sql": """
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_folga_updated_at') THEN
                    CREATE TRIGGER trg_folga_updated_at
                    BEFORE UPDATE ON folga
                    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
                END IF;
            END $$;
        """,
    },
]


# ──────────────────────────────────────────────────────────────────────────────
# EXECUÇÃO PRINCIPAL
# ──────────────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*55}")
    print(f"  Caravan Supabase — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*55}\n")

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    print(f"✅ Conectado | {len(STATEMENTS)} statements\n")

    erros = []

    for i, item in enumerate(STATEMENTS, start=1):
        try:
            cur.execute(item["sql"])
            print(f"  ✅ [{i:02d}] {item['label']}")
        except Exception as e:
            print(f"  ❌ [{i:02d}] {item['label']} → {e}")
            erros.append((item["label"], str(e)))

    if erros:
        conn.rollback()
        print(f"\n⚠️  Revertido — {len(erros)} erro(s):")
        for label, msg in erros:
            print(f"     {label}: {msg}")
        conn.close()
        sys.exit(1)

    conn.commit()

    # Relatório final
    cur.execute("""
        SELECT table_name,
               pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS tamanho
        FROM   information_schema.tables
        WHERE  table_schema = 'public'
          AND  table_name   = ANY(%s)
        ORDER  BY table_name;
    """, (["motoristas", "escala", "largadas", "ferias", "folga"],))

    print(f"\n{'─'*40}")
    print(f"  {'Tabela':<22} {'Tamanho':>8}")
    print(f"{'─'*40}")
    for nome, tamanho in cur.fetchall():
        print(f"  {nome:<22} {tamanho:>8}")
    print(f"{'─'*40}")
    print(f"\n✅ Concluído\n")

    conn.close()


if __name__ == "__main__":
    main()
