bench-freshness:
	python -m src.lambda_driver --config env/config.yaml
bench-queries:
	python -m src.bench_queries --config env/config.yaml
bench-size:
	python -m src.bench_sizes --config env/config.yaml