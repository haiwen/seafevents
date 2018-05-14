set -e

export PYTHONPATH=$(pwd)/tests:$(pwd)/seafobj:$(pwd)/ci/portable-python-libevent/libevent/
export SEAFILE_CONF_DIR=$(pwd)

cd tests
pytest -sv events
