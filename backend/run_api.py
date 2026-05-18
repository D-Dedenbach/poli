from fastapi import FastAPI, Query, HTTPException
from typing import List, Dict, Any
from datetime import datetime
import duckdb

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


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

@app.get("/polls/latest", response_model=List[Dict[str, Any]])
def get_latest_polls(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(20, le=100, description="Number of records to return (max 100)"),
    ):

    """
    Fetch the latest polls, ordered by meeting date and poll ID. 
    Paginated: returns 'limit' records, skipping 'skip' records
    """
    query = """
        SELECT poll_id
        , poll_type
        , meeting_date
        , meeting_title
        , adopted
        , case_step_title
        , case_step_status
        , case_step_type
        , case_title
        , case_title_short
        , decision
        , case_category
        , case_reasoning
        , case_status
        , for_votes
        , against_votes
        , absent_votes
        , abstain_votes
        , for_against_proportionality
    FROM dev.app_poll_outcome
    ORDER BY meeting_date DESC, poll_id DESC
    LIMIT ? OFFSET ?;
    """
    try:
        result = conn.execute(query, [limit, skip]).fetchall()

        # Convert to list of dicts (handle DuckDB's nested structures)
        if not result:
            return []
        
        columns = [desc[0] for desc in conn.description]
        polls = [dict(zip(columns, row)) for row in result]

        return polls
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")



