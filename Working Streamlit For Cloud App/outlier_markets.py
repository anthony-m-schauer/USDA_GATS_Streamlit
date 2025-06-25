

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

    query = f"SELECT * FROM {table} WHERE hs10_code = '{hs10_code}'"
    df = pd.read_sql(query, conn)

    if df.empty:
        print(f"\n✅ No data found for HS10 code: {hs10_code}")
        return None

    for _, row in df.iterrows():
        country = row["country_name"]
        entries = []

        for year in years:
            col = f"value{year}"
            if pd.isna(row[col]):
                continue

            sub_query = f"SELECT {col} FROM {table} WHERE hs10_code = '{hs10_code}' AND {col} IS NOT NULL"
            sub_df = pd.read_sql(sub_query, conn)
            
            mean = sub_df[col].mean()
            std = sub_df[col].std()
            val = row[col]
            if std == 0 or pd.isna(std):
                continue
            z_score = (val - mean) / std
            direction = "High" if z_score > 0 else "Low"
            if abs(z_score) > 2:
                entries.append(f"{year} ({direction})")

        if entries:
            outlier_summary[country] = ', '.join(entries)

    cursor.close()
    conn.close()

    result_df = pd.DataFrame(list(outlier_summary.items()), columns=["Country", "Outlier Years"])

    return result_df


##### Step 3: Run the Script
if __name__ == "__main__":

    hs10_code = input("\nEnter HS10 code: ").strip()

    print(f"\n🔍 Generating outlier summary for: {hs10_code}")
    get_outlier_markets(hs10_code, table)

    print(f"\n🏁 Done \n")
