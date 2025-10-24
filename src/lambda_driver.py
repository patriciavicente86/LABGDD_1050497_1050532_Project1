# src/lambda_driver.py
import argparse
import yaml

from clean_to_silver import run as run_silver
from features_to_gold import run as run_gold

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--with-bronze", action="store_true",
                   help="Also run bronze ingest before silver and gold.")
    return p.parse_args()

def read_cfg(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

if __name__ == "__main__":
    args = parse_args()
    cfg = read_cfg(args.config)

    if args.with_bronze:
        try:
            # Expecting src/ingest_bronze.py to expose run(cfg)
            from ingest_bronze import run as run_bronze
        except ImportError as e:
            raise RuntimeError(
                "Requested --with-bronze but couldn't import ingest_bronze.run. "
                "Make sure src/ingest_bronze.py exists and exports run(cfg)."
            ) from e
        run_bronze(cfg)

    run_silver(cfg)
    run_gold(cfg)
