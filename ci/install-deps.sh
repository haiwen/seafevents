#!/bin/bash

set -e

apt-get update  -q=2
apt-get install python2.7 python-dev autoconf automake mysql-client -q=2
apt-get install ssh libevent-dev libcurl4-openssl-dev libglib2.0-dev uuid-dev intltool libsqlite3-dev libmysqlclient-dev libarchive-dev libtool libjansson-dev valac libfuse-dev python-dateutil cmake re2c flex sqlite3 python-pip python-simplejson git libssl-dev libldap2-dev libonig-dev -q=2


pip install -r ./requirements.txt

cd ..
git clone https://github.com/seafileltd/portable-python-libevent.git

git clone https://github.com/haiwen/seafobj.git

git clone https://github.com/haiwen/libsearpc.git
cd libsearpc
./autogen.sh && ./configure && make && make install
cd ..

wget https://launchpad.net/libmemcached/1.0/1.0.18/+download/libmemcached-1.0.18.tar.gz
tar xf libmemcached-1.0.18.tar.gz
cd libmemcached-1.0.18/
./configure
make
make install
cd ..

git clone https://github.com/haiwen/libevhtp.git
cd libevhtp
cmake -DEVHTP_DISABLE_SSL=ON -DEVHTP_BUILD_SHARED=OFF . && make && make install
cd ..

git clone https://github.com/seafileltd/ccnet-pro-server.git
cd ccnet-pro-server
./autogen.sh && ./configure --enable-ldap && make && make install
cd ..

git clone https://github.com/seafileltd/seafile-pro-server.git
cd seafile-pro-server
./autogen.sh && ./configure && make && make install
