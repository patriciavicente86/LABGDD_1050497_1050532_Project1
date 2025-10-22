bench-freshness:
\tpython -m src.lambda_driver --config env/config.yaml
bench-queries:
\tpython -m src.bench_queries --config env/config.yaml
bench-size:
\tpython -m src.bench_sizes --config env/config.yaml