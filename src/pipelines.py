"""
Pipeline registry: Define all available dlt pipelines.

Each pipeline maps a unique key to:
- name: Human-readable pipeline name
- dataset: DuckDB dataset name where data is loaded
- source: Callable that returns a dlt source

Add new pipelines here as you expand the datamodel.
"""

from .sources import ft_dk_aktør_source


# Pipeline registry: {key: {name, dataset, source}}
PIPELINES = {
    "aktør": {
        "name": "ft_dk_pipeline_aktør",
        "dataset": "raw_aktør",
        "source": ft_dk_aktør_source,
        "description": "Danish parliament actors (Aktør, AktørTyper, AktørAktør, AktørAktørRolle)",
    },
    # Add more pipelines here as you expand:
    # "sag": {
    #     "name": "ft_dk_pipeline_sag",
    #     "dataset": "raw_sag",
    #     "source": ft_dk_sag_source,
    #     "description": "Danish parliament cases (Sag, SagStatusType, etc.)",
    # },
    # "afstemning": {
    #     "name": "ft_dk_pipeline_afstemning",
    #     "dataset": "raw_afstemning",
    #     "source": ft_dk_afstemning_source,
    #     "description": "Danish parliament votes (Afstemning, Stemme, etc.)",
    # },
}

