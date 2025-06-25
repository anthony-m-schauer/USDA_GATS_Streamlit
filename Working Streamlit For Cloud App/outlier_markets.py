

##### Choose a Table to Connect ----- OPTIONS: test_table ; hs10_cleaned ----- Example code: 0201206000
table = "hs10_cleaned"


##### Step 0: Imports 
import pandas as pd
from db_connection import connect_to_sql 
import re
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable') 


##### Step 1: Get List of Trade Years
def get_years_from_columns(cursor, table, schema="public"):
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s AND table_schema = %s
    """, (table, schema))
    
    columns = [col[0] for col in cursor.fetchall()]
    
    years = sorted([
        int(re.search(r'\d{4}', col).group())
        for col in columns
        if col and col.startswith('value') and re.search(r'\d{4}', col)
    ])
    
    return years


##### Step 2: Outlier Summary 
def get_outlier_markets(hs10_code, table):
    
    conn = connect_to_sql()
    cursor = conn.cursor()
    years = get_years_from_columns(cursor, table)
    outlier_summary = {}

    df = pd.read_sql(
        f"SELECT * FROM {table} WHERE hs10_code = %s AND outlier IS NOT NULL",
        conn,
        params=(hs10_code,)
    )

    if df.empty:
        print(f"\n✅ No outliers found for HS10 code: {hs10_code}")
        return

    for _, row in df.iterrows():
        country = row["country_name"]
        entries = []

        for year in years:
            col = f"value{year}"
            if pd.isna(row[col]):
                continue

            outlier_years = str(row['outlier']).split(',')
            if str(year) not in outlier_years:
                continue

            sub_df = pd.read_sql(
                f"SELECT value{year} FROM {table} WHERE hs10_code = '{hs10_code}' AND value{year} IS NOT NULL",
                conn
            )
            mean = sub_df[f"value{year}"].mean()
            val = row[col]
            direction = "High" if val > mean else "Low"
            entries.append(f"{year} ({direction})")

        if entries:
            outlier_summary[country] = ', '.join(entries)

    result_df = pd.DataFrame(list(outlier_summary.items()), columns=["Country", "Outlier Years"])
    cursor.close()
    conn.close()
    
    return result_df

##### Step 3: Run the Script
if __name__ == "__main__":

    hs10_code = input("\nEnter HS10 code: ").strip()

    print(f"\n🔍 Generating outlier summary for: {hs10_code}")
    get_outlier_markets(hs10_code, table)

    print(f"\n🏁 Done \n")
