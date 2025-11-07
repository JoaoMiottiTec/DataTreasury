import os
import sys
from pathlib import Path

import psycopg


def run_sql_file(sql_path: str) -> None:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL is not defining"
        )
    path = Path(sql_path)
    if not path.exists():
        raise FileExistsError(f"SQL is not found: {path}")
    
    sql_text = path.read_text(encoding="utf-8")

    print(f"[sql-runner] Conectando em {db_url!r}")
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            print(f"[sql-runner] Executando {path.name} ({len(sql_text)} bytes)")
            cur.execute(sql_text)
        conn.commit()
    print("[sql-runner] ✅ OK")

def main(argv: list[str]) -> None:
    if len(argv) < 2:
        print("Uso: python ops/sql_runner.py <arquivo.sql>")
        sys.exit(1)

    sql_path = argv[1]
    try:
        run_sql_file(sql_path)
    except Exception as e:
                print(f"[sql-runner] ❌ ERRO: {e}")
                sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)