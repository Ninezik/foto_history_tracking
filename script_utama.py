# %%
print('hello world')

# %%
# %%
import json
import requests
import pandas as pd
import psycopg2


# %%
from sqlalchemy import create_engine

# %%
server = 'bansosreport-db.cmfru4yoszrg.ap-southeast-3.rds.amazonaws.com'          # atau IP server
database = 'DB_NIPOS'
username = 'admin'
password = 'B4ns05dB'

engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=yes"
    "&TrustServerCertificate=yes"
)

# %%
conn = psycopg2.connect(
    host="pos-redshift.cwig526q7i0q.ap-southeast-3.redshift.amazonaws.com",
    dbname="posind_kurlog",
    user="rda_analis",
    password="GcTz69eZ6UwNnRhypjx9Ysk8",
    port=5439
)

query="""SELECT connote__connote_code,connote__created_at
            FROM nipos.nipos 
            WHERE customer_code = 'ASRBPJSKES06750A' 
            AND connote__connote_state in('DELIVERED (RETURN DELIVERY)','DELIVERED')
            AND DATE(nipos.pod__timereceive)<current_date"""

df_data_set=pd.read_sql(query,conn)

data_set= set(df_data_set['connote__connote_code'].unique())
# %%
query_mssql="""SELECT DISTINCT connote_code FROM connote_pod_new"""

# %%
df_mssql=pd.read_sql(query_mssql, engine)

# %%
df_mssql_set = set(df_mssql['connote_code'].unique())

# %%
left_only_set = data_set - df_mssql_set

# %%
len(left_only_set)

# %%
import requests
import pandas as pd
import time

all_data = []
failed = []

headers = { "Accept": "application/json", 
           "X-API-KEY": "CpFzVJWl7MgFLWT8wL8Aqz1Jvo3zyytF" }

for i, j in enumerate(left_only_set):

    url = f"https://apiexpos.mile.app/public/v2/connote/{j}"

    try:
        response = requests.get(url, headers=headers, timeout=5)

        if response.status_code == 200:
            data = response.json()
            all_data.append(data)
            print(f"Sukses: {i}")
        else:
            print(f"Gagal: {i} - Status {response.status_code}")
            failed.append(j)
            break

        time.sleep(0.5)

    except :
        print(f"Timeout terjadi di index {i} - loop dihentikan")
        failed.append(j)
        break   # ðŸ”¥ langsung stop loopin

print("Total sukses:", len(all_data))
print("Total gagal:", len(failed))


# %%
rows = []

for item in all_data:

    connote_code = item.get("connote_code")
    signature=item.get('pod').get('signature')

    for history in item.get("connote_history", []):
        if history.get("photo"):   # hanya yang ada photo

            rows.append({
                "connote_code": connote_code,
                'created_at': history.get("created_at"),
                "state": history.get("state"),
                "photo": history.get("photo"),
                'signature': signature
            })


# %%
import pandas as pd

# df = pd.DataFrame(rows)


# %%
# df2=pd.DataFrame()
df2=pd.DataFrame(rows)

# %%
# df2=pd.concat([df2, df], ignore_index=True)

# %%
df2.drop_duplicates(inplace=True)

df2['created_at'] = pd.to_datetime(df2['created_at'])
df2=pd.merge(df_data_set,df2,left_on='connote__connote_code', right_on='connote_code')
df2.drop(columns=['connote__connote_code'],inplace=True)

# %%
df2.to_sql("connote_pod_new", engine, if_exists="append", index=False,chunksize=1000)
