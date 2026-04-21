import duckdb
import csv
from backend.configs.database import DB_PATH

fetch_qry = """
SELECT case_id, case_type, case_title, case_reasoning, case_status, now() as extract_time
       FROM dev.app_par_session_polls
       GROUP BY 1, 2, 3, 4, 5
       ORDER BY random()
       LIMIT 100;
"""
conn = duckdb.connect(database=DB_PATH)
df = conn.execute(fetch_qry).fetchdf()

df.to_csv("labelling_extract_20260419.csv",sep=",",quotechar='"',index=False,
            quoting=csv.QUOTE_ALL)

