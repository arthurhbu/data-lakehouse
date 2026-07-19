import os
import duckdb
import psycopg2
from pyiceberg.catalog import load_catalog
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

load_dotenv()
console = Console()

def get_postgres_count(table_name):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "datalakehouse"),
        user=os.getenv("POSTGRES_USER", "lakehouse"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme_pg"),
    )
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cur.fetchone()[0]
    conn.close()
    return count

def get_bronze_count(table_name, pk):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    minio_user = os.getenv("MINIO_ROOT_USER")
    minio_pass = os.getenv("MINIO_ROOT_PASSWORD")
    con.execute(f"""
        CREATE SECRET minio_secret(
            TYPE S3, KEY_ID '{minio_user}', SECRET '{minio_pass}',
            REGION 'us-east-1', ENDPOINT 'localhost:9000',
            USE_SSL false, URL_STYLE 'path'
        );
    """)
    path = f"s3://bronze/topics/cdc.public.{table_name}/*/*/*/*.json"
    try:
        # Pega a contagem deduplicada da Bronze (simulando a mesma lógica da Silver)
        res = con.execute(f"""
            SELECT COUNT(DISTINCT payload.after.{pk}) 
            FROM read_json_auto('{path}', ignore_errors=true) 
            WHERE payload.after IS NOT NULL
        """).fetchone()[0]
        return res
    except Exception as e:
        return 0

def get_silver_count(table_name):
    try:
        catalog = load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": "http://localhost:8181/",
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": os.getenv("MINIO_ROOT_USER"),
                "s3.secret-access-key": os.getenv("MINIO_ROOT_PASSWORD"),
            },
        )
        table = catalog.load_table(f"silver.{table_name}")
        # Converte para arrow table e pega o número exato de linhas
        return len(table.scan().to_arrow())
    except Exception as e:
        return 0

def main():
    tables = [
        {"name": "transactions", "pk": "transaction_id"},
        {"name": "payment_events", "pk": "event_id"},
        {"name": "ledger_entries", "pk": "entry_id"}
    ]
    
    t = Table(title="Reconciliação Ponta-a-Ponta (Zero Perda de Dados)")
    t.add_column("Tabela", style="cyan")
    t.add_column("Postgres (Origem)", style="magenta", justify="right")
    t.add_column("Bronze CDC (MinIO)", style="yellow", justify="right")
    t.add_column("Silver (Iceberg)", style="blue", justify="right")
    t.add_column("Status", justify="center")
    
    for tb in tables:
        pg_c = get_postgres_count(tb["name"])
        br_c = get_bronze_count(tb["name"], tb["pk"])
        sl_c = get_silver_count(tb["name"])
        
        status = "[bold green]✓ Sincronizado[/bold green]" if pg_c == br_c == sl_c else "[bold red]✗ Divergente[/bold red]"
        t.add_row(tb["name"], str(pg_c), str(br_c), str(sl_c), status)
        
    console.print(t)
    console.print("\n[yellow]DICA:[/yellow] Se a Silver estiver atrasada, lembre-se de rodar 'make silver' para processar os dados da Bronze.")

if __name__ == "__main__":
    main()
