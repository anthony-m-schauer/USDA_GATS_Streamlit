
##### Choose a Table to Connect ----- OPTIONS: hs10_test ; hs10_cleaned

table = "hs10_cleaned"


##### Step 0: Imports
import pandas as pd
from db_connection import connect_to_sql 
import re
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')



##### Step 1: Calculate Multi-Year Averages (3, 5, 10, 15)
def get_average_exports(hs10_code, table):
    
    conn = connect_to_sql()

    try:
        query = f"SELECT * FROM {table} WHERE hs10_code = '{hs10_code}'"
        df = pd.read_sql(query, conn, params=(hs10_code,))

        if df.empty:
            print("No data found.")
            return None

        value_cols = [col for col in df.columns if re.match(r"value\d{4}", col)]
        df_long = df.melt(id_vars=["country_code"], value_vars=value_cols,
                          var_name="year", value_name="value")
        df_long["year"] = df_long["year"].str.extract(r"(\d{4})").astype(int)
        df_long = df_long[df_long["value"].notnull()]

        def avg(df, label, years=None):
            if years:
                df = df[df["year"].isin(years)]
            
            avg_data = (
                df.groupby("country_code")["value"]
                .mean()
                .reset_index()
                .rename(columns={"value": "average_value"})
            )

            avg_data["period"] = label
            avg_data = avg_data.sort_values("average_value", ascending=False).head(10)
            return avg_data
        
        years = sorted(df_long["year"].unique())
        last_3 = years[-3:] if len(years) >= 3 else years
        last_5 = years[-5:] if len(years) >= 5 else years
        last_10 = years[-10:] if len(years) >= 10 else years
        last_15 = years[-15:] if len(years) >= 15 else years

        avg_all = avg(df_long, "All Time")
        avg_15 = avg(df_long, "Last 15 Years", last_15)
        avg_10 = avg(df_long, "Last 10 Years", last_10)
        avg_5 = avg(df_long, "Last 5 Years", last_5)
        avg_3 = avg(df_long, "Last 3 Years", last_3)

        return avg_all, avg_15, avg_10, avg_5, avg_3
    

    except Exception as err:
        print(f"❌ Error: {err}")
        return None
    finally:
        conn.close()

##### Step 2: Run Lines
if __name__ == "__main__":
    print("\n📊 USDA GATS — Multi-Year Average Export Values\n")
    hs_code = input("Enter an HS-10 code: ").strip()

    result = get_average_exports(hs_code, table)

    labels = ["Last 3 Years", "Last 5 Years", "Last 10 Years", "Last 15 Years", "All Time"]
    reversed_results = result[::-1]  # Reverse the tuple

    if result is not None:
        for df, label in zip(reversed_results, labels):
            print(f"\n🔹 {label} Top 10:\n")
            print(df.to_string(index=False))
    else:
        print("\n⚠️ No results returned.\n")

