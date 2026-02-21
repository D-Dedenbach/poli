"""
dlt sources for Danish parliament OData API.

Uses dlt's rest_api source for automatic pagination, schema inference, and normalization.
"""

import dlt
from dlt.sources.rest_api import rest_api_source


@dlt.source
def ft_dk_source():
    """Source for Danish parliament OData API with automatic pagination."""
    
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
                "name": "actors",
                "endpoint": "Aktør",
                "write_disposition": "replace",
            },
            {
                "name": "actor_types",
                "endpoint": "AktørTyper",
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
        ],
    }
    
    return rest_api_source(config)
