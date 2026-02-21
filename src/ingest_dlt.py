"""
dlt pipeline: Ingest Danish parliament data from OData API to DuckDB.

dlt's rest_api_source handles:
- Pagination automatically
- Schema inference & evolution
- Data normalization

Usage:
    python -m src.ingest_dlt              # Load all resources
    python -m src.ingest_dlt --resources actors  # Load only specific resources
"""

import argparse
import os
import dlt
from .sources import ft_dk_source


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Danish parliament data using dlt"
    )
    parser.add_argument(
        "--resources",
        nargs="+",
        default=None,
        choices=["actors", "actor_types", "actor_actor", "actor_actor_roles"],
        help="Which resources to load (default: all)",
    )
    args = parser.parse_args()

    # Set database path to data/ folder (compute from this file's location)
    root_dir = os.path.dirname(os.path.dirname(__file__))
    db_path = os.path.join(root_dir, "data", "data.duckdb")
    
    # Create pipeline - DuckDB destination
    # pipelines_dir should be root so schemas are organized at project level
    pipeline = dlt.pipeline(
        pipeline_name="ft_dk_pipeline",
        destination="duckdb",
        dataset_name="raw",
        pipelines_dir=root_dir,
    )

    # Get source
    source = ft_dk_source()

    # Filter resources if specified
    if args.resources:
        source = source.with_resources(*args.resources)

    print(f"Loading resources: {args.resources if args.resources else 'all'}")
    
    # Run pipeline
    load_info = pipeline.run(source)
    
    print("\n✓ Load completed")
    print(f"Dataset: {pipeline.dataset_name}")
    print(f"Database: data/data.duckdb")
    
    if load_info.loads_ids:
        print(f"✓ Successfully loaded data")
    else:
        print("⚠ Some loads may have failed")


if __name__ == "__main__":
    main()
