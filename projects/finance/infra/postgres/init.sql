-- =============================================================================
-- Data Lakehouse — DDL do domínio 
-- Clearing House internacional: pagamentos cross-border, câmbio, double-entry
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------------------------------------------------------
-- partners: empresas que operam pela clearing (Amazon, Shopify, bancos, etc.)
-- -----------------------------------------------------------------------------
CREATE TABLE partners (
    partner_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name         VARCHAR(200)  NOT NULL,
    country      CHAR(2)       NOT NULL,          -- ISO 3166-1 alpha-2
    partner_type VARCHAR(30)   NOT NULL            -- merchant | bank | processor
                     CHECK (partner_type IN ('merchant','bank','processor')),
    status       VARCHAR(20)   NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active','suspended','inactive')),
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- accounts: contas financeiras dos parceiros (uma por moeda/tipo)
-- -----------------------------------------------------------------------------
CREATE TABLE accounts (
    account_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    partner_id   UUID          NOT NULL REFERENCES partners(partner_id),
    currency     CHAR(3)       NOT NULL,           -- ISO 4217 (USD, BRL, EUR …)
    account_type VARCHAR(20)   NOT NULL             -- settlement | holding | fee
                     CHECK (account_type IN ('settlement','holding','fee')),
    status       VARCHAR(20)   NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active','frozen','closed')),
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX idx_accounts_partner ON accounts(partner_id);

-- -----------------------------------------------------------------------------
-- transactions: intenção de movimentação financeira (cross-border)
-- -----------------------------------------------------------------------------
CREATE TABLE transactions (
    transaction_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_account_id    UUID          NOT NULL REFERENCES accounts(account_id),
    destination_account_id UUID        NOT NULL REFERENCES accounts(account_id),
    amount               NUMERIC(18,4) NOT NULL,   -- valor original
    currency             CHAR(3)       NOT NULL,    -- moeda original
    fx_rate              NUMERIC(18,8),             -- taxa de câmbio aplicada
    converted_amount     NUMERIC(18,4),             -- valor após conversão
    converted_currency   CHAR(3),                   -- moeda destino
    payment_method       VARCHAR(20)   NOT NULL     -- wire | card | pix | boleto
                             CHECK (payment_method IN ('wire','card','pix','boleto')),
    status               VARCHAR(20)   NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending','authorized','completed',
                                                'voided','refunded')),
    description          TEXT,
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX idx_transactions_status     ON transactions(status);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);
CREATE INDEX idx_transactions_source     ON transactions(source_account_id);
CREATE INDEX idx_transactions_dest       ON transactions(destination_account_id);

-- -----------------------------------------------------------------------------
-- payment_events: ciclo de vida de cada transação (initiated → settled/voided)
-- -----------------------------------------------------------------------------
CREATE TABLE payment_events (
    event_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id UUID         NOT NULL REFERENCES transactions(transaction_id),
    event_type     VARCHAR(30)  NOT NULL
                       CHECK (event_type IN ('initiated','authorized','captured',
                                              'settled','voided','refunded',
                                              'chargeback')),
    metadata       JSONB,                          -- dados extras por tipo de evento
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_payment_events_txn ON payment_events(transaction_id);
CREATE INDEX idx_payment_events_type ON payment_events(event_type);

-- -----------------------------------------------------------------------------
-- ledger_entries: livro-razão (double-entry)
--
-- Regra de ouro: para cada transação, SUM(amount) = 0
--   débito  = valor positivo
--   crédito = valor negativo
-- -----------------------------------------------------------------------------
CREATE TABLE ledger_entries (
    entry_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id UUID          NOT NULL REFERENCES transactions(transaction_id),
    account_id     UUID          NOT NULL REFERENCES accounts(account_id),
    entry_type     VARCHAR(10)   NOT NULL
                       CHECK (entry_type IN ('debit','credit')),
    amount         NUMERIC(18,4) NOT NULL,         -- +debit / -credit
    currency       CHAR(3)       NOT NULL,
    description    TEXT,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX idx_ledger_txn     ON ledger_entries(transaction_id);
CREATE INDEX idx_ledger_account ON ledger_entries(account_id);

-- =============================================================================
-- PUBLICATION para CDC (Debezium lê as mudanças destas tabelas via WAL)
-- =============================================================================
CREATE PUBLICATION lakehouse_cdc FOR TABLE
    partners,
    accounts,
    transactions,
    payment_events,
    ledger_entries;

-- =============================================================================
-- Trigger genérico para atualizar updated_at automaticamente
-- =============================================================================
CREATE OR REPLACE FUNCTION trigger_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at BEFORE UPDATE ON partners
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TRIGGER set_updated_at BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TRIGGER set_updated_at BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();
