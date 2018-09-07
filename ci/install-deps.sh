#!/bin/bash

set -e

apt-get update  -q=2
apt-get install python2.7 python-dev autoconf automake mysql-client -q=2
apt-get install ssh libevent-dev libcurl4-openssl-dev libglib2.0-dev uuid-dev intltool libsqlite3-dev libmysqlclient-dev libarchive-dev libtool libjansson-dev valac libfuse-dev python-dateutil cmake re2c flex sqlite3 python-pip python-simplejson git libssl-dev libldap2-dev libonig-dev -q=2

git clone https://github.com/seafileltd/portable-python-libevent.git

git clone https://github.com/haiwen/seafobj.git

pip install -r ./requirements.txt

git clone https://github.com/haiwen/libsearpc.git
cd libsearpc
./autogen.sh && ./configure && make && make install


git clone https://github.com/haiwen/ccnet-server.git

cd ccnet-server
./autogen.sh && ./configure && make && make install