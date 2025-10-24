validate:
	python -m src.clean_to_silver --config env/config.yaml
features:
	python -m src.features_to_gold --config env/config.yaml