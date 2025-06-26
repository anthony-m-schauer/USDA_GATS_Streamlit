



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
    

##### Step 2: Calculate HS10 Share of Total Market by Year
def calculate_percents_index(hs10_code, table):
    conn = connect_to_sql()
    cursor = conn.cursor()
    years = get_years_from_columns(cursor, table)
    query = f"SELECT * FROM {table}"
    df_all = pd.read_sql(query, conn)
    df_hs = df_all[df_all["hs10_code"] == hs10_code]
    results = []

    if df_hs.empty:
        print(f"⚠️ No data found for HS10 code: {hs10_code}")
        return pd.DataFrame()
    
    for year in years:
        value_col = f"value{year}"
        if value_col not in df_all.columns:
            continue

        total_market = df_all[value_col].sum(skipna=True)
        hs10_market = df_hs[value_col].sum(skipna=True)

        if pd.isna(total_market) or total_market == 0:
            percent = None
        else:
            percent = round((hs10_market / total_market) * 100, 4)

        results.append({
            "Year": year,
            "Percent Total Market": percent
        })

    results = pd.DataFrame(results).sort_values(by="Year", ascending=False)
    cursor.close()
    conn.close()
    
    return results

##### Step 3: Run Full Process
if __name__ == "__main__":
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------")
    print(f"\n                      🚀 Launching HS10 Market Share Calculator | For Table: {table}\n") 

    hs10_code = input("🔍 Enter an HS10 code to analyze: ").strip()

    percents_df = calculate_percents_index(hs10_code, table)
    
    print(f"\n📈 Market Share by Year (% of Total US Trade):\n{percents_df}")
    print("\n🏁 Done.")
    print("------------------------------------------------------------------------------------------------------------------------------------------------------------\n")

    
