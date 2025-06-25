##### Choose a Table to Connect ----- OPTIONS: hs10_full ; test_table ; countries ; hs10_cleaned
table = "hs10_cleaned"


##### Step 0: Imports 
import pandas as pd
from db_connection import connect_to_sql
import re
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')


##### Step 1: Calculate Trending Markets 
def get_trending_markets(hs10_code, table):
    
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

        def trending(df, label, years=None):
            if years:
                df = df[df["year"].isin(years)]

            trends = []
            for country, group in df.groupby("country_name"):
                group_sorted = group.sort_values("year")
                start = group_sorted.iloc[0]["value"]
                end = group_sorted.iloc[-1]["value"]
                if start > 0:
                    growth_pct = ((end - start) / start) * 100
                else:
                    growth_pct = None 
                trends.append({
                    "country_name": country,
                    "start_value": start,
                    "end_value": end,
                    "growth_%": growth_pct,
                    "period": label
                })

            trend_df = pd.DataFrame(trends)
            trend_df = trend_df.dropna(subset=["growth_%"])
            trend_df = trend_df.sort_values("growth_%", ascending=False).head(10)
            return trend_df

        years = sorted(df_long["year"].unique())
        last_10 = years[-10:] if len(years) >= 10 else years
        last_5 = years[-5:] if len(years) >= 5 else years

        trend_all = trending(df_long, "All Time")
        trend_10 = trending(df_long, "Last 10 Years", last_10)
        trend_5 = trending(df_long, "Last 5 Years", last_5)
        conn.close()

        return trend_all, trend_10, trend_5

    except Exception as err:
        print(f"❌ Error: {err}")
        return None


##### Step 2: Run Lines
if __name__ == "__main__":
    print("\n📈 USDA GATS — Trending Markets\n")
    hs_code = input("Enter an HS-10 code: ").strip()

    result = get_trending_markets(hs_code, table)

    if result is not None:
        print("\n🚀 Trending Markets by Growth (%):\n")
        print(result)
    else:
        print("\n⚠️ No results returned.\n")