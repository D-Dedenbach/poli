from fastapi import FastAPI
import duckdb
import os 

app = FastAPI()

db_path = os.path.join(os.path.dirname(__file__), "../data/data.duckdb")
conn = duckdb.connect(database=db_path, read_only=True)

@app.get("/votes/{actor_id}")
def get_votes(actor_id: int):
    query = f"""
    SELECT 
        vote_id
        , poll_id
        , actor_id
        , actor_name
        , actor_type_id
        , updated_at
    FROM raw_app.app_votes
    WHERE actor_id = {actor_id}
    ORDER BY updated_at DESC
    """
    result = conn.execute(query).fetchall()

    if not result:
        return {"actor_id": actor_id, "votes": []}
    
    return {"actor_id": actor_id, "votes": result}
