import argparse
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from faker import Faker
from rich.console import Console
from rich.table import Table
from rich.progress import Progress

load_dotenv()
console = Console()
fake = Faker()

# ---------------------------------------------------------------------------
# Conexão
# ---------------------------------------------------------------------------

def get_connection():
    try:
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            dbname=os.getenv("POSTGRES_DB", "datalakehouse"),
            user=os.getenv("POSTGRES_USER", "lakehouse"),
            password=os.getenv("POSTGRES_PASSWORD", "changeme_pg"),
        )
    except Exception as e:
        console.print(f"[bold red]Erro ao conectar ao Postgres:[/bold red] {e}")
        raise

# ---------------------------------------------------------------------------
# Dados de referência
# ---------------------------------------------------------------------------

COUNTRIES = ["US", "CA", "BR", "NL", "GB", "DE", "JP", "CH"]

CURRENCIES_BY_COUNTRY = {
    "US": "USD", "CA": "CAD", "BR": "BRL",
    "NL": "EUR", "GB": "GBP", "DE": "EUR",
    "JP": "JPY", "CH": "CHF"
}

FX_RATES = {
    ("USD", "BRL"): Decimal("5.15"),
    ("USD", "EUR"): Decimal("0.92"),
    ("USD", "GBP"): Decimal("0.79"),
    ("USD", "CAD"): Decimal("1.36"),
    ("USD", "JPY"): Decimal("155.0"),
    ("USD", "CHF"): Decimal("0.91"),
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

def seed_partners_and_accounts(cur, num_partners=10):
    """Cria parceiros e contas (settlement + holding + fee) se ainda não existirem."""
    cur.execute("SELECT COUNT(*) FROM partners")
    existing_count = cur.fetchone()[0]
    
    if existing_count > 0:
        console.print("[yellow]Parceiros já existem — pulando seed.[/yellow]")
        return

    partner_data = []
    for _ in range(num_partners):
        pid = str(uuid.uuid4())
        name = fake.company()
        country = random.choice(COUNTRIES)
        ptype = random.choice(['merchant', 'bank', 'processor'])
        partner_data.append((pid, name, country, ptype))

    execute_values(
        cur,
        "INSERT INTO partners (partner_id, name, country, partner_type) VALUES %s",
        partner_data
    )

    account_data = []
    for pid, name, country, ptype in partner_data:
        currency = CURRENCIES_BY_COUNTRY[country]
        for acc_type in ("settlement", "holding", "fee"):
            aid = str(uuid.uuid4())
            account_data.append((aid, pid, currency, acc_type))

    execute_values(
        cur,
        "INSERT INTO accounts (account_id, partner_id, currency, account_type) VALUES %s",
        account_data
    )

    console.print(f"[green]Seed concluído:[/green] {len(partner_data)} parceiros, {len(account_data)} contas criados.")


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
        jitter = Decimal(str(random.uniform(-0.02, 0.02)))
        return (FX_RATES[key] + jitter).quantize(Decimal("0.00000001")), dst_currency
    
    # Inverso se disponível
    inv_key = (dst_currency, src_currency)
    if inv_key in FX_RATES:
        rate = (Decimal("1.0") / FX_RATES[inv_key])
        jitter = Decimal(str(random.uniform(-0.02, 0.02)))
        return (rate + jitter).quantize(Decimal("0.00000001")), dst_currency
        
    return Decimal("1.0"), src_currency


def generate_transactions(cur, num_transactions):
    """Gera N transações com payment_events e ledger_entries."""
    accounts = load_accounts(cur)
    if len(accounts) < 2:
        console.print("[red]Erro: precisa de pelo menos 2 contas settlement. Rode --seed-only primeiro.[/red]")
        return

    with Progress() as progress:
        task = progress.add_task("[cyan]Gerando transações...", total=num_transactions)
        
        for _ in range(num_transactions):
            src = random.choice(accounts)
            dst = random.choice([a for a in accounts if a[0] != src[0]])
            src_id, src_cur = src
            dst_id, dst_cur = dst

            amount = Decimal(str(random.uniform(10, 10000))).quantize(Decimal("0.01"))
            fx_rate, conv_currency = pick_fx_rate(src_cur, dst_cur)
            converted = (amount * fx_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            method = random.choice(PAYMENT_METHODS)

            base_time = datetime.now(timezone.utc) - timedelta(
                days=random.randint(0, 5),
                hours=random.randint(0, 23),
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
                    f"Transferencia {src_cur} para {dst_cur}",
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
                    f'{{"method":"{method}","fx_rate":"{fx_rate}","source":"generator"}}',
                    base_time,
                ),
            )

            # --- ledger_entries (double-entry: débito + crédito = 0) ---
            # Para simplificar o modelo de estudo, mantemos ambos na moeda de origem
            cur.execute(
                """INSERT INTO ledger_entries
                   (entry_id, transaction_id, account_id, entry_type,
                    amount, currency, description, created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    str(uuid.uuid4()), txn_id, src_id, "debit",
                    amount, src_cur,
                    f"Débito (saída) {src_cur}",
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
                    f"Crédito (entrada) {src_cur}",
                    base_time,
                ),
            )
            
            progress.update(task, advance=1)


# ---------------------------------------------------------------------------
# Validação: invariante Soma Zero
# ---------------------------------------------------------------------------

def validate_sum_zero(cur):
    """Verifica a invariante de ouro: SUM(ledger_entries.amount) = 0."""
    cur.execute("SELECT COALESCE(SUM(amount), 0) FROM ledger_entries")
    total = cur.fetchone()[0]
    if total == 0:
        console.print(f"[bold green]Invariante Soma Zero: OK (total = {total})[/bold green]")
    else:
        console.print(f"[bold red]ALERTA: Soma Zero violada! total = {total}[/bold red]")
    return total == 0

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Gerador de dados para Data Lakehouse")
    parser.add_argument(
        "--transactions", type=int, default=50,
        help="Número de transações a gerar (default: 50)",
    )
    parser.add_argument(
        "--partners", type=int, default=10,
        help="Número de parceiros a criar se não existirem (default: 10)",
    )
    parser.add_argument(
        "--seed-only", action="store_true",
        help="Criar apenas parceiros e contas, sem transações",
    )
    parser.add_argument(
        "--continuous", action="store_true",
        help="Executar em modo contínuo inserindo transações infinitamente"
    )
    parser.add_argument(
        "--delay", type=float, default=2.0,
        help="Atraso em segundos entre cada transação no modo contínuo"
    )
    args = parser.parse_args()

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        seed_partners_and_accounts(cur, args.partners)
        conn.commit()

        if not args.seed_only:
            if args.continuous:
                import time
                console.print(f"[bold green]Iniciando modo contínuo (1 transação a cada {args.delay}s)... Pressione Ctrl+C para parar.[/bold green]")
                try:
                    count = 0
                    while True:
                        # Gera sem a barra de progresso para não poluir o terminal
                        generate_transactions(cur, 1)
                        conn.commit()
                        count += 1
                        print(f"Transações inseridas: {count}", end="\r")
                        time.sleep(args.delay)
                except KeyboardInterrupt:
                    console.print("\n[yellow]Modo contínuo interrompido pelo usuário.[/yellow]")
            else:
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

        table = Table(title="Resumo do Banco de Dados")
        table.add_column("Entidade", style="cyan")
        table.add_column("Total de Registros", style="magenta")
        
        table.add_row("Parceiros", str(p))
        table.add_row("Contas", str(a))
        table.add_row("Transações", str(t))
        table.add_row("Ledger entries", str(l))
        
        console.print(table)

    except Exception as e:
        conn.rollback()
        console.print(f"[bold red]Erro durante a execução:[/bold red] {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
