include make/ingest.mk
include make/validate.mk
include make/bench.mk

build:
\tdocker build -t nyc-taxi-offline -f docker/Dockerfile .

dev:
\tdocker run --rm -it -p 8888:8888 -v $$PWD:/workspace nyc-taxi-offline bash

lab:
\tjupyter lab --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.token=''