"""
Data Lakehouse — Gerador de dados simulados (domínio GlobalNexus).

Popula o Postgres com parceiros, contas e transações cross-border realistas.
Cada transação gera automaticamente:
  - 1 payment_event (initiated)
  - 2 ledger_entries (débito na origem, crédito no destino → soma zero)

Uso:
  python ingestion/generator.py                    # 50 transações (padrão)
  python ingestion/generator.py --transactions 200 # 200 transações
  python ingestion/generator.py --seed-only        # apenas parceiros e contas
"""

import argparse
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Conexão
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "datalakehouse"),
        user=os.getenv("POSTGRES_USER", "lakehouse"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme_pg"),
    )

# ---------------------------------------------------------------------------
# Dados de referência
# ---------------------------------------------------------------------------

PARTNERS = [
    ("Amazon Global Payments",  "US", "merchant"),
    ("Shopify International",   "CA", "merchant"),
    ("Mercado Livre",           "BR", "merchant"),
    ("Stripe Payments",         "US", "processor"),
    ("Adyen BV",                "NL", "processor"),
    ("Banco Itaú",              "BR", "bank"),
    ("HSBC Holdings",           "GB", "bank"),
    ("Deutsche Bank",           "DE", "bank"),
]

CURRENCIES_BY_COUNTRY = {
    "US": "USD", "CA": "CAD", "BR": "BRL",
    "NL": "EUR", "GB": "GBP", "DE": "EUR",
}

FX_RATES = {
    ("USD", "BRL"): Decimal("5.15"),
    ("USD", "EUR"): Decimal("0.92"),
    ("USD", "GBP"): Decimal("0.79"),
    ("USD", "CAD"): Decimal("1.36"),
    ("BRL", "USD"): Decimal("0.194"),
    ("BRL", "EUR"): Decimal("0.179"),
    ("EUR", "USD"): Decimal("1.087"),
    ("EUR", "BRL"): Decimal("5.59"),
    ("GBP", "USD"): Decimal("1.266"),
    ("CAD", "USD"): Decimal("0.735"),
}

PAYMENT_METHODS = ["wire", "card", "pix", "boleto"]

# ---------------------------------------------------------------------------
# Seed: parceiros + contas
# ---------------------------------------------------------------------------

def seed_partners_and_accounts(cur):
    """Cria parceiros e contas (settlement + holding) se ainda não existirem."""
    cur.execute("SELECT COUNT(*) FROM partners")
    if cur.fetchone()[0] > 0:
        print("Parceiros já existem — pulando seed.")
        return

    partner_ids = []
    for name, country, ptype in PARTNERS:
        pid = str(uuid.uuid4())
        cur.execute(
            """INSERT INTO partners (partner_id, name, country, partner_type)
               VALUES (%s, %s, %s, %s)""",
            (pid, name, country, ptype),
        )
        partner_ids.append((pid, country))

    account_ids = []
    for pid, country in partner_ids:
        currency = CURRENCIES_BY_COUNTRY[country]
        for acc_type in ("settlement", "holding"):
            aid = str(uuid.uuid4())
            cur.execute(
                """INSERT INTO accounts (account_id, partner_id, currency, account_type)
                   VALUES (%s, %s, %s, %s)""",
                (aid, pid, currency, acc_type),
            )
            account_ids.append((aid, currency))

    print(f"Seed: {len(PARTNERS)} parceiros, {len(account_ids)} contas criados.")
    return account_ids


def load_accounts(cur):
    """Carrega contas settlement existentes para geração de transações."""
    cur.execute(
        "SELECT account_id, currency FROM accounts WHERE account_type = 'settlement'"
    )
    return cur.fetchall()

# ---------------------------------------------------------------------------
# Gerador de transações
# ---------------------------------------------------------------------------

def pick_fx_rate(src_currency, dst_currency):
    if src_currency == dst_currency:
        return Decimal("1.0"), src_currency
    key = (src_currency, dst_currency)
    if key in FX_RATES:
        jitter = Decimal(str(random.uniform(-0.03, 0.03)))
        return (FX_RATES[key] + jitter).quantize(Decimal("0.00000001")), dst_currency
    return Decimal("1.0"), src_currency


def generate_transactions(cur, num_transactions):
    """Gera N transações com payment_events e ledger_entries."""
    accounts = load_accounts(cur)
    if len(accounts) < 2:
        print("Erro: precisa de pelo menos 2 contas settlement. Rode --seed-only primeiro.")
        return

    tx_count = 0
    for _ in range(num_transactions):
        src = random.choice(accounts)
        dst = random.choice([a for a in accounts if a[0] != src[0]])
        src_id, src_cur = src
        dst_id, dst_cur = dst

        amount = Decimal(str(random.uniform(100, 50_000))).quantize(Decimal("0.01"))
        fx_rate, conv_currency = pick_fx_rate(src_cur, dst_cur)
        converted = (amount * fx_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        method = random.choice(PAYMENT_METHODS)

        base_time = datetime.now(timezone.utc) - timedelta(
            hours=random.randint(0, 72),
            minutes=random.randint(0, 59),
        )

        txn_id = str(uuid.uuid4())

        # --- transaction ---
        cur.execute(
            """INSERT INTO transactions
               (transaction_id, source_account_id, destination_account_id,
                amount, currency, fx_rate, converted_amount, converted_currency,
                payment_method, status, description, created_at, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                txn_id, src_id, dst_id,
                amount, src_cur, fx_rate, converted, conv_currency,
                method, "completed",
                f"Cross-border {src_cur}→{dst_cur}",
                base_time, base_time,
            ),
        )

        # --- payment_event (initiated) ---
        cur.execute(
            """INSERT INTO payment_events
               (event_id, transaction_id, event_type, metadata, created_at)
               VALUES (%s,%s,%s,%s,%s)""",
            (
                str(uuid.uuid4()), txn_id, "initiated",
                f'{{"method":"{method}","fx_rate":"{fx_rate}"}}',
                base_time,
            ),
        )

        # --- ledger_entries (double-entry: débito + crédito = 0) ---
        cur.execute(
            """INSERT INTO ledger_entries
               (entry_id, transaction_id, account_id, entry_type,
                amount, currency, description, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                str(uuid.uuid4()), txn_id, src_id, "debit",
                amount, src_cur,
                f"Débito saída {src_cur}",
                base_time,
            ),
        )
        cur.execute(
            """INSERT INTO ledger_entries
               (entry_id, transaction_id, account_id, entry_type,
                amount, currency, description, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                str(uuid.uuid4()), txn_id, dst_id, "credit",
                -amount, src_cur,
                f"Crédito entrada {src_cur}",
                base_time,
            ),
        )

        tx_count += 1

    print(f"Gerados: {tx_count} transações, {tx_count} payment_events, "
          f"{tx_count * 2} ledger_entries.")


# ---------------------------------------------------------------------------
# Validação: invariante Soma Zero
# ---------------------------------------------------------------------------

def validate_sum_zero(cur):
    """Verifica a invariante de ouro: SUM(ledger_entries.amount) = 0."""
    cur.execute("SELECT COALESCE(SUM(amount), 0) FROM ledger_entries")
    total = cur.fetchone()[0]
    if total == 0:
        print(f"Invariante Soma Zero: OK (total = {total})")
    else:
        print(f"ALERTA: Soma Zero violada! total = {total}")
    return total == 0

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Gerador de dados GlobalNexus")
    parser.add_argument(
        "--transactions", type=int, default=50,
        help="Número de transações a gerar (default: 50)",
    )
    parser.add_argument(
        "--seed-only", action="store_true",
        help="Criar apenas parceiros e contas, sem transações",
    )
    args = parser.parse_args()

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        seed_partners_and_accounts(cur)
        conn.commit()

        if not args.seed_only:
            generate_transactions(cur, args.transactions)
            conn.commit()

        validate_sum_zero(cur)

        cur.execute("SELECT COUNT(*) FROM partners")
        p = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM accounts")
        a = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM transactions")
        t = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM ledger_entries")
        l = cur.fetchone()[0]

        print(f"\nResumo do banco:")
        print(f"  Parceiros:       {p}")
        print(f"  Contas:          {a}")
        print(f"  Transações:      {t}")
        print(f"  Ledger entries:  {l}")

    except Exception as e:
        conn.rollback()
        print(f"Erro: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
