import argparse
import duckdb
import os
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from silver_schemas import TABLE_CONFIGS

load_dotenv()

class SilverJob:
    def __init__(self, table_name: str, primary_key: str, schema, timestamp_col: str = None):
        self.table_name = table_name
        self.primary_key = primary_key
        self.timestamp_col = timestamp_col
        self.schema = schema
        
        self.con = duckdb.connect()
        self._setup_extensions_and_secret()
        self._setup_iceberg_catalog()

    def _setup_iceberg_catalog(self):
        print(f"[{self.table_name}] Conectando ao Iceberg REST catalog...")
        self.catalog = load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": "http://localhost:8181/",
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": os.getenv("MINIO_ROOT_USER"),
                "s3.secret-access-key": os.getenv("MINIO_ROOT_PASSWORD"),
            },
        )
        namespaces = [ns[0] for ns in self.catalog.list_namespaces()]
        if "silver" not in namespaces:
            self.catalog.create_namespace("silver", properties={"location": "s3://silver/"})

    def _setup_extensions_and_secret(self):
        self.con.execute("INSTALL httpfs; LOAD httpfs;")
        minio_user = os.getenv("MINIO_ROOT_USER")
        minio_pass = os.getenv("MINIO_ROOT_PASSWORD")
        self.con.execute(f"""
            CREATE SECRET minio_secret(
                TYPE S3,
                KEY_ID '{minio_user}',
                SECRET '{minio_pass}',
                REGION 'us-east-1',
                ENDPOINT 'localhost:9000',
                USE_SSL false,
                URL_STYLE 'path'
            );
        """)

    def retrieve_data(self):
        print(f"[{self.table_name}] Buscando dados na camada Bronze...")
        path = f"s3://bronze/topics/cdc.public.{self.table_name}/*/*/*/*.json"
        
        from pyiceberg.types import (
            DoubleType, UUIDType, TimestampType, StringType, IntegerType, BooleanType
        )

        cast_exprs = []
        for f in self.schema.fields:
            if isinstance(f.field_type, UUIDType):
                db_type = "UUID"
            elif isinstance(f.field_type, DoubleType):
                db_type = "DOUBLE"
            elif isinstance(f.field_type, TimestampType):
                db_type = "TIMESTAMP"
            elif isinstance(f.field_type, StringType):
                db_type = "VARCHAR"
            elif isinstance(f.field_type, IntegerType):
                db_type = "BIGINT"
            elif isinstance(f.field_type, BooleanType):
                db_type = "BOOLEAN"
            else:
                db_type = "VARCHAR"
            cast_exprs.append(f"CAST(payload.after.{f.name} AS {db_type}) AS {f.name}")

        select_cast_exprs = ", ".join(cast_exprs)
        print(f"Colunas com CAST: {cast_exprs}")
        # Deduplica usando a chave dinâmica.
        self.con.execute(f"""
        CREATE OR REPLACE TEMP VIEW bronze_clean AS
        SELECT {select_cast_exprs}
        FROM read_json_auto('{path}')
        WHERE payload.after IS NOT NULL
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY payload.after.{self.primary_key}
            ORDER BY payload.source.lsn DESC
        ) = 1
        """)

    def run(self):
        self.retrieve_data()
        
        # Cria a partição dinamicamente baseada na coluna de timestamp do contrato
        if self.timestamp_col:
            ts_field = self.schema.find_field(self.timestamp_col)
            partition_spec = PartitionSpec(
                PartitionField(source_id=ts_field.field_id, field_id=1000, transform=DayTransform(), name=f"{self.timestamp_col}_day")
            )
        else:
            # Tabela dimensional sem data (Unpartitioned)
            partition_spec = PartitionSpec()
        
        table_path = f"silver.{self.table_name}"
        table_exists = True
        try:
            table = self.catalog.load_table(table_path)
            print(f"[{self.table_name}] Tabela Iceberg encontrada. Preparando MERGE (Upsert)...")
        except:
            table = self.catalog.create_table(
                table_path,
                schema=self.schema,
                partition_spec=partition_spec,
                properties={"write.target-file-size-bytes": "134217728"}
            )
            print(f"[{self.table_name}] Tabela Iceberg criada do zero!")
            table_exists = False

        import pyarrow as pa
        import uuid
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        from pyiceberg.types import UUIDType
        target_pyarrow_schema = schema_to_pyarrow(table.schema())

        if not table_exists:
            print(f"[{self.table_name}] Carga inicial: Inserindo dados...")
            cols = ", ".join([f.name for f in self.schema.fields])
            arrow_table = self.con.execute(f"SELECT {cols} FROM bronze_clean").to_arrow_table()
            
            # Cast das colunas UUID de string para binário de 16 bytes
            converted_columns = []
            for f in self.schema.fields:
                col = arrow_table.column(f.name)
                if isinstance(f.field_type, UUIDType):
                    py_vals = col.to_pylist()
                    bytes_vals = [uuid.UUID(v).bytes if v is not None else None for v in py_vals]
                    converted_columns.append(pa.array(bytes_vals, type=pa.binary(16)))
                else:
                    converted_columns.append(col)
            
            arrow_table = pa.Table.from_arrays(converted_columns, names=arrow_table.column_names)
            arrow_table = arrow_table.cast(target_pyarrow_schema)
            table.append(arrow_table)
        else:
            print(f"[{self.table_name}] Fazendo o MERGE (Full Outer Join)...")
            silver_atual = table.scan().to_arrow()
            self.con.register("silver_atual_view", silver_atual)
            
            # Monta os COALESCE dinamicamente baseados nas colunas do schema
            coalesce_exprs = []
            for f in self.schema.fields:
                if f.name == self.primary_key:
                    coalesce_exprs.append(f"COALESCE(b.{f.name}, s.{f.name}) AS {f.name}")
                else:
                    coalesce_exprs.append(
                        f"CASE WHEN b.{self.primary_key} IS NOT NULL THEN b.{f.name} ELSE s.{f.name} END AS {f.name}"
                    )
            select_cols = ", ".join(coalesce_exprs)
            
            merge_query = f"""
                SELECT {select_cols}
                FROM silver_atual_view s
                FULL OUTER JOIN bronze_clean b
                ON s.{self.primary_key} = b.{self.primary_key}
            """
            
            tabela_consolidada_arrow = self.con.execute(merge_query).to_arrow_table()
            
            # Cast das colunas UUID de string para binário de 16 bytes
            converted_columns = []
            for f in self.schema.fields:
                col = tabela_consolidada_arrow.column(f.name)
                if isinstance(f.field_type, UUIDType):
                    py_vals = col.to_pylist()
                    bytes_vals = [uuid.UUID(v).bytes if v is not None else None for v in py_vals]
                    converted_columns.append(pa.array(bytes_vals, type=pa.binary(16)))
                else:
                    converted_columns.append(col)
            
            tabela_consolidada_arrow = pa.Table.from_arrays(converted_columns, names=tabela_consolidada_arrow.column_names)
            tabela_consolidada_arrow = tabela_consolidada_arrow.cast(target_pyarrow_schema)
            table.overwrite(tabela_consolidada_arrow)
            
        print(f"[{self.table_name}] Concluído com Sucesso!")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="Nome da tabela para processar")
    args = parser.parse_args()

    table_name = args.table
    config = TABLE_CONFIGS.get(table_name)
    if not config:
        raise ValueError(f"Tabela '{table_name}' não encontrada nos schemas.")

    job = None
    try:
        job = SilverJob(
            table_name=table_name,
            primary_key=config["primary_key"],
            schema=config["schema"],
            timestamp_col=config.get("timestamp_col")
        )
        job.run()
    except Exception as e:
        print(f"Erro Crítico: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if job is not None:
            job.con.close()

if __name__ == "__main__":
    main()
