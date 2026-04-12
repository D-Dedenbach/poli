"""
dlt pipeline runner: Execute Danish parliament data pipelines.

dlt's rest_api_source handles:
- Pagination automatically
- Schema inference & evolution
- Data normalization

Usage:
    python -m src.ingest_dlt actors                    # Load actor data
    python -m src.ingest_dlt votes                     # Load voting data
    python -m src.ingest_dlt actors --resources actors # Load specific resources
"""

import argparse
import os
import sys
import dlt
from .sources import ft_dk_actor_source, ft_dk_afstemning_source, ft_dk_sag_source, ft_dk_mode_source


# Map source names to their functions
SOURCES = {
    "actors": {
        "function": ft_dk_actor_source,
        "description": "Danish parliament actors (Aktør, AktørType, AktørAktør, AktørAktørRolle)",
    },
    "votes": {
        "function": ft_dk_afstemning_source,
        "description": "Danish parliament votes (Afstemning, Stemme, Stemmetype, Afstemningstype)",
    },
    "cases": {
        "function": ft_dk_sag_source,  # Placeholder for future case source
        "description": "Danish parliament cases (Sag, Sagsstatus, Sagstype, )"
    },
    "meeting": {
        "function": ft_dk_mode_source,
        "description": "Parliamentary meetings in plenum and committees (Møde, Mødetype, Mødestatus)"
    }
}


def run_pipeline(source_name: str, resources: list[str] | None = None):
    """
    Run dlt pipeline to load Danish parliament data into DuckDB.

    Args:
        source_name: Name of the source to run (e.g., 'actors', 'votes')
        resources: List of specific resources to load, or None for all
    """
    # Validate source exists
    if source_name not in SOURCES:
        print(f"❌ Source '{source_name}' not found")
        print(f"Available sources: {', '.join(SOURCES.keys())}")
        return None
    
    source_config = SOURCES[source_name]
    
    # Compute paths relative to project root
    root_dir = os.path.dirname(os.path.dirname(__file__))
    data_dir = os.path.join(root_dir, "data")
    db_path = os.path.join(data_dir, "data.duckdb")
    
    # Ensure data directory exists
    os.makedirs(data_dir, exist_ok=True)
    
    # Set DuckDB database path via environment variable before creating pipeline
    os.environ['DESTINATION__DUCKDB__CREDENTIALS__DATABASE'] = db_path
    
    # Create pipeline with unified 'raw' schema
    pipeline = dlt.pipeline(
        pipeline_name=f"ft_dk_{source_name}",
        destination="duckdb",
        dataset_name="raw",
        progress=dlt.progress.log(10)
    )
    
    # Get the source
    source = source_config["function"]()
    
    # Filter to specific resources if requested
    if resources:
        source = source.with_resources(*resources)
    
    # Print pipeline info
    print(f"📦 Running pipeline: {source_name}")
    print(f"   Description: {source_config['description']}")
    print(f"   Dataset: raw")
    resource_list = resources if resources else "all"
    print(f"   Resources: {resource_list}\n")
    
    # Run the pipeline with live progress updates
    load_info = pipeline.run(source)
    
    return load_info


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Danish parliament data using dlt",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.ingest_dlt actors                    # Load all actor resources
  python -m src.ingest_dlt votes                     # Load all vote resources
  python -m src.ingest_dlt actors --resources actors # Load only actors
  python -m src.ingest_dlt --list                    # Show available sources
        """,
    )
    parser.add_argument(
        "source",
        nargs="?",
        default=None,
        help="Source to run ('actors', 'votes', 'cases', 'meetings' or 'all'), or '--list' to show available",
    )
    parser.add_argument(
        "--resources",
        nargs="+",
        default=None,
        help="Specific resources to load from the source",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available sources",
    )


    args = parser.parse_args()
    

    # Handle --list flag
    if args.list or args.source == "list":
        print("Available sources:")
        for key, config in SOURCES.items():
            print(f"  • {key:15} — {config['description']}")
        return

    # Require source argument
    if not args.source:
        parser.print_help()
        sys.exit(1)

    # Handle 'all' sources
    if args.source.lower() == "all":
        if args.resources:
            print("❌ --resources only works with a single source")
            sys.exit(1)
        print(f"Running all {len(SOURCES)} sources...\n")
        for source_name in SOURCES.keys():
            print(f"\n{'='*60}")
            load_info = run_pipeline(source_name)
            if load_info:
                print(f"✅ {source_name} completed successfully")
        return

    # Run single source
    load_info = run_pipeline(args.source, args.resources)
    
    if load_info:
        print(f"\n✅ Pipeline completed successfully!")
        print(f"📊 Database: data/data.duckdb")
        print(f"📈 Load info:\n{load_info}")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
