import pandas as pd
import sys
rd = sys.argv[1]
sv = sys.argv[2]
df = pd.read_csv(rd, on_bad_lines = 'skip', encoding = 'latin1', names = ['dt', 'cmd', 'cnt'])

for ind in df.columns:
    df[ind] = df[ind].str.replace('^','')
df['cnt'] = pd.to_numeric(df['cnt'], errors='coerce')
df['cnt'] = df['cnt'].fillna(0).astype(int)

df.to_parquet(sv, partition_cols=['dt'])
