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
    WITH grouped_polls AS (
        SELECT
            poll_id,
            title,
            meeting_date,
            poll_type,
            adopted,
            JSON_GROUP_ARRAY(
                JSON_OBJECT(
                        'party_abbr', party_abbr,
                        'vote_type', vote_type,
                        'vote_count', vote_count
                )
            ) AS parties
        FROM dev.app_poll_outcome
        GROUP BY poll_id, title, meeting_date, poll_type, adopted
    )
    SELECT
        poll_id,
        title,
        meeting_date,
        poll_type,
        adopted,
        parties
    FROM grouped_polls
    ORDER BY meeting_date DESC
    LIMIT ? OFFSET ?
    """
    try:
        result = conn.execute(query, [limit, skip]).fetchall()

        # Convert to list of dicts (handle DuckDB's nested structures)
        polls = []
        for row in result:
            poll_dict = {
                "poll_id": row[0],
                "title": row[1],
                "meeting_date": row[2].isoformat() if isinstance(row[2], datetime) else row[2],
                "poll_type": row[3],
                "adopted": row[4],
                "parties": row[5],  # row[5] is the 'parties' array
                
            }
            polls.append(poll_dict)

        return polls
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")



