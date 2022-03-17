import pandas as pd
import time
from sqlalchemy import create_engine

t0=time.perf_counter()
df = pd.read_csv('./test_files/hw_random.csv', header = 0)
t1=time.perf_counter() - t0
print(f"Time elapsed to read csv: {t1} seconds")

engine = create_engine('mysql://root:MySql2019!@localhost/testepythondb')
with engine.connect() as conn, conn.begin():
    df.to_sql('height_weight', conn, if_exists='append', index=False, chunksize=200)

t2=time.perf_counter() - t1
print(f"Time elapsed to write sql: {t2} seconds")