# Entry-point script to run the data pipeline stages: silver and gold.
# Optionally runs a bronze ingest step before silver/gold when --with-bronze is provided.
# Usage: python lambda_driver.py --config path/to/config.yaml [--with-bronze]

import argparse
import yaml

# Import stage runners (expected to expose a run(cfg) function)
from clean_to_silver import run as run_silver
from features_to_gold import run as run_gold

def parse_args():
    """
    Parse command-line arguments.
    --config: path to a YAML configuration file (required)
    --with-bronze: optional flag to run the bronze ingest step first
    """
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True,
                   help="Path to YAML configuration file used by pipeline stages.")
    p.add_argument("--with-bronze", action="store_true",
                   help="Also run bronze ingest before silver and gold.")
    return p.parse_args()

def read_cfg(path):
    """
    Read and parse the YAML configuration from the given path.
    Returns a Python dict representing the configuration.
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)

if __name__ == "__main__":
    # Parse CLI args and load configuration
    args = parse_args()
    cfg = read_cfg(args.config)

    # If requested, attempt to import and run the bronze ingest stage.
    # This import is done here so ingest_bronze is only required when the flag is used.
    if args.with_bronze:
        try:
            # Expecting src/ingest_bronze.py (or module on PYTHONPATH) to expose run(cfg)
            from ingest_bronze import run as run_bronze
        except ImportError as e:
            # Provide a clear error message if the optional module is missing or fails to import.
            raise RuntimeError(
                "Requested --with-bronze but couldn't import ingest_bronze.run. "
                "Make sure src/ingest_bronze.py exists and exports run(cfg)."
            ) from e
        # Execute bronze ingest with the loaded configuration
        run_bronze(cfg)

    # Run the main pipeline stages in order: silver, then gold.
    run_silver(cfg)
    run_gold(cfg)
