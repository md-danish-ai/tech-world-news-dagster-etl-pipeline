# query.py
import sqlite3
import pandas as pd

# Connect to your SQLite database
conn = sqlite3.connect("data/hackernews.sqlite")

# Configure Pandas to display wide, complete tables
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)
pd.set_option("display.width", 1000)

print("=" * 60)
print(" HACKER NEWS SQLITE QUERY CONSOLE")
print("Type your SQL query below (or type 'exit' to quit).")
print("=" * 60)

while True:
    try:
        query = input("\nSQL> ").strip()
        if not query:
            continue
        if query.lower() in ("exit", "quit", "q"):
            break

        # Run query and display results
        df = pd.read_sql(query, conn)
        print(f"\n Results ({len(df)} rows):\n")
        print(df.to_string(index=False))

    except Exception as e:
        print(f" Error: {e}")

conn.close()
