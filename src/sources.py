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

from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.sources.rest_api.config_setup import register_paginator


class ODataOffsetPaginator(OffsetPaginator):
    """Custom paginator for OData APIs that use $skip and $top for pagination."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_total(self, response):
        # OData APIs often don't provide a total count, so return None
        return None

    def get_items(self, response):
        # Items are in the 'value' key for OData APIs
        return response.json().get("value", [])

    def exhausted(self, response):
        # Stop pagination if 'value' is empty
        return not response.json().get("value", [])

register_paginator("odata_offset", ODataOffsetPaginator)



@dlt.source
def ft_dk_actor_source():
    """
    Source for Danish parliament actor data from OData API.

    Fetches actor-related tables:
    - actors: Core actor data (persons, parties, committees, etc.)
    - actor_types: Actor type definitions
    - actor_actor: Relationships between actors
    - actor_actor_roles: Actor roles in relationships

    Uses automatic pagination via offset/limit.
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
                "endpoint": "Aktør",
                "write_disposition": "replace",
            },
            {
                "name": "actor_types",
                "endpoint": "Aktørtype",
                "write_disposition": "replace",
            },
            {
                "name": "actor_actor",
                "endpoint": "AktørAktør",
                "write_disposition": "replace",
            },
            {
                "name": "actor_actor_roles",
                "endpoint": "AktørAktørRolle",
                "write_disposition": "replace",
            },
        ]

    }
    
    return rest_api_source(config)


# Future sources (uncomment and implement as needed)
# @dlt.source
# def ft_dk_sag_source():
#     """
#     Source for Danish parliament case data from OData API.
#
#     Fetches case-related tables:
#     - cases: Core case data
#     - case_status_types: Case status definitions
#     - ... (add more as needed)
#     """
#     config = {
#         "client": {
#             "base_url": "https://oda.ft.dk/api",
#             "paginator": {
#                 "type": "offset",
#                 "limit": 100,
#                 "limit_param": "$top",
#                 "offset_param": "$skip",
#             },
#         },
#         "resources": [
#             {
#                 "name": "cases",
#                 "endpoint": "Sag",
#                 "write_disposition": "replace",
#             },
#             # Add more resources as needed
#         ],
#     }
#     return rest_api_source(config)


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
            },
        },
        "resources": [
            {
                "name": "votes",
                "endpoint": "Afstemning",
                "write_disposition": "replace",
            },
            # Add more resources as needed
        ],
    }
    return rest_api_source(config)

