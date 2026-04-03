"""
dlt sources for Danish parliament OData API.

Uses dlt's rest_api source for automatic pagination, schema inference, and normalization.

Each source function corresponds to a major entity in the Folketinget datamodel:
- ft_dk_aktør_source: Actors and actor relationships
- (future) ft_dk_sag_source: Cases and case metadata
- (future) ft_dk_afstemning_source: Votes and voting records
"""

import dlt
from dlt.sources.rest_api import rest_api_source


@dlt.source
def ft_dk_actor_source():
    """
    Source for Danish parliament actor data from OData API.

    Fetches actor-related tables:
    - actors: Core actor data (persons, parties, committees, etc.)
    - actor_types: Actor type definitions
    - actor_actor: Relationships between actors
    - actor_actor_roles: Actor roles in relationships

    Uses automatic pagination via offset/limit and incremental loading via opdateringsdato.
    """

    config = {
        "client": {
            "base_url": "https://oda.ft.dk/api",
            "paginator": {
                "type": "offset",
                "total_path": None, # OData APIs often don't provide total count, so we set this to None
                "limit": 100,
                "limit_param": "$top",
                "offset_param": "$skip"
            },
        },
        "resources": [
            {
                "name": "actors",
                "endpoint": "Aktør?$filter=opdateringsdato ge datetime'2020-01-01T00:00:00'",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "actor_types",
                "endpoint": "Aktørtype",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "actor_actor",
                "endpoint": "AktørAktør?$filter=opdateringsdato ge datetime'2020-01-01T00:00:00'",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "actor_actor_roles",
                "endpoint": "AktørAktørRolle",
                "write_disposition": "merge",
                "primary_key": "id",
            },
        ]

    }
    
    return rest_api_source(config)


@dlt.source
def ft_dk_afstemning_source():
    """
    Source for Danish parliament voting data from OData API.

    Fetches voting-related tables:
    - votes: Core vote data
    - vote_types: Vote type definitions
    - ... (add more as needed)
    """
    config = {
        "client": {
            "base_url": "https://oda.ft.dk/api",
            "paginator": {
                "type": "offset",
                "limit": 100,
                "limit_param": "$top",
                "offset_param": "$skip",
                "total_path": None, # OData APIs often don't provide total count, so we set this to None
            },
        },
        "resources": [
            {
                "name": "votes",
                "endpoint": "Afstemning?$filter=opdateringsdato ge datetime'2020-01-01T00:00:00'",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "member_votes",
                "endpoint": "Stemme?$filter=opdateringsdato ge datetime'2020-01-01T00:00:00'",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "member_vote_types",
                "endpoint": "Stemmetype",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "vote_types",
                "endpoint": "Afstemningstype",
                "write_disposition": "merge",
                "primary_key": "id",
            }
        ],
    }
    return rest_api_source(config)

@dlt.source
def ft_dk_sag_source():
    """
    Source for Danish parliament case data from OData API.

    Fetches case-related tables:
    - cases: Core case data
    - case_status_types: Case status definitions
    - ... (add more as needed)
    """
    config = {
        "client": {
            "base_url": "https://oda.ft.dk/api",
            "paginator": {
                "type": "offset",
                "limit": 100,
                "limit_param": "$top",
                "offset_param": "$skip",
                "total_path": None, # OData APIs often don't provide total count, so we set this to None
            },
        },
        "resources": [
            {
                "name": "case",
                "endpoint": "Sag?$filter=opdateringsdato ge datetime'2020-01-01T00:00:00'",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "case_status",
                "endpoint": "Sagsstatus",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "case_type",
                "endpoint": "Sagstype",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "case_category",
                "endpoint": "Sagskategori",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "case_step",
                "endpoint": "Sagstrin?$filter=opdateringsdato ge datetime'2020-01-01T00:00:00'",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "case_step_type",
                "endpoint": "Sagstrinstype",
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "case_step_status",
                "endpoint": "Sagstrinsstatus",
                "write_disposition": "merge",
                "primary_key": "id",
            },
        ],
    }
    return rest_api_source(config)

