# make/pipeline.mk
PY ?= python
CONFIG ?= env/config.yaml

.PHONY: silver gold lambda kappa pipeline all clean

silver:
	@$(PY) -m src.clean_to_silver --config $(CONFIG)

gold:
	@$(PY) -m src.features_to_gold --config $(CONFIG)

lambda: silver gold
pipeline: silver gold
all: pipeline

kappa:
	@$(PY) -m src.kappa_driver --config $(CONFIG)

clean:
	@rm -rf checkpoints/* tmp/* || true
