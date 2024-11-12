import os
import pymysql
import pandas as pd
from dotenv import load_dotenv

i_file_path = r'res/02_incident.csv'
df_i = pd.read_csv(i_file_path)

dp_file_path = r'res/03_diff_party.csv'
df_dp = pd.read_csv(dp_file_path)

load_dotenv()
config = {
    "host": os.getenv('DB_HOST'),
    "port": int(os.getenv('DB_PORT')),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "db": os.getenv('DB_NAME'),
    "charset":  os.getenv('DB_CHARSET'),
}

conn = pymysql.connect(**config)
cursor = conn.cursor()

# truncate diff_party
sql_truncate = """
truncate diff_party;
"""
cursor.execute(sql_truncate)
conn.commit()

# insert diff_party
sql_insert = """
insert into diff_party
values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

values = list(df_dp.itertuples(index=False, name=None))
cursor.executemany(sql_insert, values)

# truncate incident
sql_truncate = """
truncate incident;
"""
cursor.execute(sql_truncate)
conn.commit()

# insert incident
sql_insert= """
insert into incident
values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

values = list(df_i.itertuples(index=False, name=None))

cursor.executemany(sql_insert, values)
conn.commit()

cursor.close()
conn.close()