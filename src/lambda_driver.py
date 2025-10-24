# src/lambda_driver.py
import argparse, yaml
from clean_to_silver import run as run_silver
from features_to_gold import run as run_gold

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)
    run_silver(cfg)
    run_gold(cfg)
