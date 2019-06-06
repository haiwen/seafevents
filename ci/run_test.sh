set -e

export PYTHONPATH=/drone/src/github.com/seafileltd/seafile-pro-server/python:/usr/local/lib/python2.7/dist-packages:$(pwd)/tests:/drone/src/github.com/seafileltd/seafobj:/drone/src/github.com/seafileltd/portable-python-libevent/libevent/
export SEAFILE_CONF_DIR=/drone/src/github.com/seafileltd/seafile-pro-server/tests/conf/seafile-data
export CCNET_CONF_DIR=/drone/src/github.com/seafileltd/seafile-pro-server/tests/conf

cd tests
pytest -sv events
