"""
dlt pipeline runner: Execute Danish parliament data pipelines.

dlt's rest_api_source handles:
- Pagination automatically
- Schema inference & evolution
- Data normalization

Usage:
    python -m src.ingest_dlt aktør               # Run specific pipeline
    python -m src.ingest_dlt aktør --resources actors  # Load specific resources
    python -m src.ingest_dlt --list              # List all available pipelines
    python -m src.ingest_dlt all                 # Run all pipelines
"""

import argparse
import os
import sys
import dlt
from .pipelines import PIPELINES


def run_pipeline(pipeline_key: str, resources: list[str] | None = None):
    """
    Run a single dlt pipeline.

    Args:
        pipeline_key: Key in PIPELINES dict (e.g., 'aktør')
        resources: List of specific resources to load, or None for all

    Returns:
        load_info from dlt.pipeline.run()
    """
    if pipeline_key not in PIPELINES:
        print(f"❌ Pipeline '{pipeline_key}' not found")
        print(f"Available pipelines: {', '.join(PIPELINES.keys())}")
        return None

    config = PIPELINES[pipeline_key]
    root_dir = os.path.dirname(os.path.dirname(__file__))

    # Create pipeline - DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name=config["name"],
        destination="duckdb",
        dataset_name=config["dataset"],
        pipelines_dir=root_dir,
    )

    # Get source
    source = config["source"]()

    # Filter resources if specified
    if resources:
        # Validate resources
        available = [r.name for r in source.resources.values()]
        invalid = [r for r in resources if r not in available]
        if invalid:
            print(f"❌ Invalid resources: {', '.join(invalid)}")
            print(f"Available: {', '.join(available)}")
            return None
        source = source.with_resources(*resources)

    print(f"📦 Running pipeline: {config['name']}")
    print(f"   Dataset: {config['dataset']}")
    resource_list = resources if resources else "all"
    print(f"   Resources: {resource_list}")

    # Run pipeline
    load_info = pipeline.run(source)
    
    print(f"✓ Pipeline '{config['name']}' completed")
    if load_info.loads_ids:
        print(f"✓ Successfully loaded data")
    else:
        print("⚠ Some loads may have failed")

    return load_info


def main():
    parser = argparse.ArgumentParser(
        description="Run Danish parliament dlt pipelines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.ingest_dlt aktør                    # Run aktør pipeline
  python -m src.ingest_dlt aktør --resources actors # Load only actors
  python -m src.ingest_dlt all                      # Run all pipelines
  python -m src.ingest_dlt --list                   # Show available pipelines
        """,
    )
    parser.add_argument(
        "pipeline",
        nargs="?",
        default=None,
        help="Pipeline to run, or 'all' to run all, or 'list' to show available",
    )
    parser.add_argument(
        "--resources",
        nargs="+",
        default=None,
        help="Specific resources to load (only with single pipeline)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available pipelines",
    )

    args = parser.parse_args()

    # Handle --list flag
    if args.list or args.pipeline == "list":
        print("Available pipelines:")
        for key, config in PIPELINES.items():
            source = config["source"]()
            resources = [r.name for r in source.resources.values()]
            print(f"  • {key:15} → dataset: {config['dataset']:15} resources: {', '.join(resources)}")
        return

    # Require pipeline argument
    if not args.pipeline:
        parser.print_help()
        sys.exit(1)

    # Handle 'all' pipelines
    if args.pipeline.lower() == "all":
        if args.resources:
            print("❌ --resources only works with a single pipeline")
            sys.exit(1)
        print(f"Running all {len(PIPELINES)} pipelines...\n")
        for key in PIPELINES.keys():
            run_pipeline(key)
            print()
        return

    # Run single pipeline
    run_pipeline(args.pipeline, args.resources)


if __name__ == "__main__":
    main()
