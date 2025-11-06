import os, glob
from psycopg import connect

DB_URL = os.getenv("DATABASE_URL")
MIG_DIR = "warehouse/migrations"

def main():
    print(f"[migrate] Starting DB migrations (psycopg). DB={DB_URL}")
    files = sorted(glob.glob(f"{MIG_DIR}/*.sql"))
    with connect(DB_URL) as conn:
        for path in files:
            print(f"[migrate] Applying {os.path.basename(path)}")
            with open(path, "r", encoding="utf-8") as f:
                conn.execute(f.read())
        conn.commit()
    print("[migrate] ✅ All migrations applied.")

if __name__ == "__main__":
    main()
