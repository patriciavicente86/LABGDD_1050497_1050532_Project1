# make/pipeline.mk
PY ?= python
CONFIG ?= env/config.yaml

.PHONY: silver gold lambda kappa pipeline all clean metrics compare kappa_start kappa_seed kappa_probe kappa_seed_next

silver:
	@$(PY) -m src.clean_to_silver --config $(CONFIG)

gold:
	@$(PY) -m src.features_to_gold --config $(CONFIG)

lambda: silver gold
pipeline: silver gold
all: pipeline
	
kappa_start:
	$(PY) -m src.kappa_driver --config $(CONFIG)

clean:
	@rm -rf checkpoints/* tmp/* || true

metrics:
	$(PY) -m src.metrics --config env/config.yaml

compare:
	$(PY) -m src.compare_lambda_kappa --config env/config.yaml

kappa_seed:
	mkdir -p lake/bronze_stream
	@for svc in yellow green; do \
	  for y in 2024; do \
	    src="lake/bronze/service=$$svc/year=$$y"; \
	    dst="lake/bronze_stream/service=$$svc/year=$$y"; \
	    if [ -d "$$src" ]; then \
	      mkdir -p "$$dst"; \
	      echo "Seeding $$src -> $$dst (first 1 month)"; \
	      m=$$(ls -d $$src/month=* 2>/dev/null | head -n 1); \
	      if [ -n "$$m" ]; then mkdir -p "$$dst/$${m##*/}"; cp -n "$$m"/*.parquet "$$dst/$${m##*/}" 2>/dev/null || true; fi; \
	    fi; \
	  done; \
	done

kappa_probe:
	$(PY) -m src.probe_stream

kappa_seed_next:
	@set -euo pipefail; \
	for svc in yellow green; do \
	  src_base="lake/bronze/service=$$svc/year=2024"; \
	  dst_base="lake/bronze_stream/service=$$svc/year=2024"; \
	  [ -d "$$src_base" ] || continue; \
	  mkdir -p "$$dst_base"; \
	  # pick the first month in source that isn't in stream yet
	  for m in $$(ls -d $$src_base/month=* 2>/dev/null | sort); do \
	    mon=$${m##*/}; \
	    if [ ! -d "$$dst_base/$$mon" ]; then \
	      echo "Seeding $$m -> $$dst_base/$$mon"; \
	      mkdir -p "$$dst_base/$$mon"; \
	      cp -n "$$m"/*.parquet "$$dst_base/$$mon"/ 2>/dev/null || true; \
	      break; \
	    fi; \
	  done; \
	done