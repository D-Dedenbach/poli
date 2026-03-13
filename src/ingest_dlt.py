"""
dlt pipeline runner: Execute Danish parliament data pipelines.

dlt's rest_api_source handles:
- Pagination automatically
- Schema inference & evolution
- Data normalization

Usage:
    python -m src.ingest_dlt                           # Run all resources
    python -m src.ingest_dlt --resources actors        # Load specific resources
"""

import argparse
import os
import dlt
from .sources import ft_dk_actor_source


def run_pipeline(resources: list[str] | None = None):
    """
    Run dlt pipeline to load Danish parliament data into DuckDB.

    Args:
        resources: List of specific resources to load, or None for all
    """
    # Compute paths relative to project root
    root_dir = os.path.dirname(os.path.dirname(__file__))
    data_dir = os.path.join(root_dir, "data")
    db_path = os.path.join(data_dir, "data.duckdb")
    
    # Ensure data directory exists
    os.makedirs(data_dir, exist_ok=True)
    
    # Set DuckDB database path via environment variable before creating pipeline
    os.environ['DESTINATION__DUCKDB__CREDENTIALS__DATABASE'] = db_path
    
    # Single pipeline for all resources
    pipeline = dlt.pipeline(
        pipeline_name="ft_dk_parliament",
        destination="duckdb",
        dataset_name="raw"
    )
    
    # Get the source
    source = ft_dk_actor_source()
    
    # Filter to specific resources if requested
    if resources:
        source = source.with_resources(*resources)
    
    # Run the pipeline
    load_info = pipeline.run(source)
    
    return load_info


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Danish parliament data using dlt"
    )
    parser.add_argument(
        "--resources",
        nargs="+",
        default=None,
        help="Which resources to load (e.g., actors actor_types)",
    )
    
    args = parser.parse_args()
    
    print(f"Running dlt pipeline with resources: {args.resources or 'all'}")
    
    load_info = run_pipeline(resources=args.resources)
    
    print(f"\n✅ Pipeline completed successfully!")
    print(f"📊 Database: data/data.duckdb")
    print(f"📈 Load info:\n{load_info}")


if __name__ == "__main__":
    main()
