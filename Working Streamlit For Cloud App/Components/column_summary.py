##### Choose a Table to Analyze ----- OPTIONS: hs10_full ; test_table ; countries ; hs10_cleaned
table = "hs10_cleaned" 


##### Step 0: Imports 
import pandas as pd
from db_connection import connect_to_sql
import warnings
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')


##### Step 1: Column Summary Generator
def get_column_summary(df):
    summary = []
    for col in df.columns:
        data_type = df[col].dtype
        num_nulls = df[col].isnull().sum()
        num_unique = df[col].nunique()
        sample_values = ', '.join(map(str, df[col].dropna().unique()[:5])) 
        stats = {
            'column': col,
            'type': str(data_type),
            'nulls': num_nulls,
            'unique': num_unique,
            'sample_values': sample_values
        }
        if pd.api.types.is_numeric_dtype(df[col]):
            stats['mean'] = df[col].mean()
            stats['min'] = df[col].min()
            stats['max'] = df[col].max()
        summary.append(stats)
    
    return summary


##### Step 2: Print Summary Generator 
def print_summary(summary):
    for stat in summary:
        print(f"Column: {stat['column']}")
        print(f"  Type: {stat['type']}")
        print(f"  Nulls: {stat['nulls']}")
        print(f"  Unique values: {stat['unique']}")
        print(f"  Sample values: {stat['sample_values']}")
        if 'mean' in stat:
            print(f"  Mean: {stat['mean']:.2f}")
            print(f"  Min: {stat['min']}")
            print(f"  Max: {stat['max']}")
        print()


##### Step 3: Run Full Column Summary 
def run_column_summary(table):
    
    conn = connect_to_sql()
    
    try:       
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, conn)
        summary = get_column_summary(df)
        print_summary(summary)
    except Exception as err:
        print(f"\n❌ Error reading table: {err}\n") 
    finally:
        conn.close()
