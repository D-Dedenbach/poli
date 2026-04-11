from fastapi import FastAPI
import duckdb

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from utils.categorization_utils import load_categories
from transform_scripts.classifier import classify_text

app = FastAPI()

db_path = os.path.join(os.path.dirname(__file__), "../data/data.duckdb")
conn = duckdb.connect(database=db_path, read_only=True)

@app.get("/votes/{actor_id}")
def get_votes(actor_id: int):
    """
    Endpoint to retrieve all votes for a given actor_id as defined on oda.ft.dk. 
    Returns a JSON object containing the actor_id and a list of votes.
    Each vote includes: vote_id, poll_id, actor_id, actor_name, actor_type_id, and updated_at.

    Currently no pagination is required, the results are ordered by updated at in inverse chronological order.
    """
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
    
    columns = [desc[0] for desc in conn.description]
    votes = [dict(zip(columns, row)) for row in result]

    return {"actor_id": actor_id, "votes": votes}



