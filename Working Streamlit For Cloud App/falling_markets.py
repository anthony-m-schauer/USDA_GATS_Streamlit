

##### Choose a Table to Connect ----- OPTIONS: hs10_full ; test_table ; countries ; hs10_cleaned
table = "hs10_cleaned"


##### Step 0: Imports
import pandas as pd
from db_connection import connect_to_sql 
import re
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')


##### Step 1: Calculate Falling Markets 
def get_falling_markets(hs10_code, table):
    
    conn = connect_to_sql()

    try:
        query = f"SELECT * FROM {table} WHERE hs10_code = %s"
        df = pd.read_sql(query, conn, params=(hs10_code,))

        if df.empty:
            print("No data found.") 
            return None

        value_cols = [col for col in df.columns if re.match(r"value\d{4}", col)]
        df_long = df.melt(id_vars=["country_name"], value_vars=value_cols,
                          var_name="year", value_name="value")
        df_long["year"] = df_long["year"].str.extract(r"(\d{4})").astype(int)
        df_long = df_long[df_long["value"].notnull()]

        def falling(df, label, years=None):
            if years:
                df = df[df["year"].isin(years)]
        
            falling = []
            for country, group in df.groupby("country_name"):
                group_sorted = group.sort_values("year")
                start = group_sorted.iloc[0]["value"]
                end = group_sorted.iloc[-1]["value"]
                if start > end and start != 0:
                    fall_pct = round(((start - end) / start) * 100, 2)
                else:
                    fall_pct = None
                falling.append({
                    "country_name": country,
                    "start_value": start,
                    "end_value": end,
                    "percent_fall": fall_pct,
                    "period": label
                })
        
            fall_df = pd.DataFrame(falling)
            fall_df = fall_df.dropna(subset=["percent_fall"])
            fall_df = fall_df.sort_values("percent_fall", ascending=False).head(10)
            return fall_df

        years = sorted(df_long["year"].unique())
        last_10 = years[-10:] if len(years) >= 10 else years
        last_5 = years[-5:] if len(years) >= 5 else years

        fall_all = falling(df_long, "All Time")
        fall_10 = falling(df_long, "Last 10 Years", last_10)
        fall_5 = falling(df_long, "Last 5 Years", last_5)

        conn.close()
        return fall_all, fall_10, fall_5

    except Exception as err:
        print(f"❌ Error: {err}")
        return None



##### Step 2: Run Lines
if __name__ == "__main__":
    print("\n📈 USDA GATS — Falling Markets\n")
    hs_code = input("Enter an HS-10 code: ").strip()

    result = get_falling_markets(hs_code, table)

    if result is not None:
        print("\n🚀 Falling Markets by Falling Percent:\n")
        print(result)
    else:
        print("\n⚠️ No results returned.\n")