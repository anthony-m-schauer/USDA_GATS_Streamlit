

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


##### Step 2: Calculate Herfindahl Index by hs10_code and year
def calculate_hhi(hs10_code, table):
    conn = connect_to_sql()
    cursor = conn.cursor()
    years = get_years_from_columns(cursor, table)
    query = f"SELECT * FROM {table} WHERE hs10_code = '{hs10_code}' AND outlier IS NOT NULL"
    df = pd.read_sql(query, conn)

    if df.empty:
        print(f"⚠️ No data found for HS10 code: {hs10_code}")
        return pd.DataFrame()

    results = []

    for year in years:
        value_col = f"value{year}"
        if value_col not in df.columns:
            continue

        sub = df[["country_code", value_col]].copy()
        sub = sub[sub[value_col].notna()]
        total = sub[value_col].sum()

        if total == 0:
            hhi = None
        else:
            shares = sub[value_col] / total
            hhi = (shares**2).sum()

        results.append({
            "Year": year,
            "HHI Index": round(hhi, 4) if hhi is not None else None
        })

    results = pd.DataFrame(results).sort_values(by="Year", ascending=False)
    cursor.close()
    conn.close()

    return results


##### Step 3: Run Lines
if __name__ == "__main__":
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------")
    print(f"\n                      🚀 Launching Herfindahl Index Calculator | For Table: {table}\n") 
    hs10_code = input("Enter an HS-10 code: ").strip()

    hhi_df = calculate_hhi(hs10_code, table)
    if hhi_df is not None and not hhi_df.empty:
        print(hhi_df)

    print("\n🏁 Done.")
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------\n")    
