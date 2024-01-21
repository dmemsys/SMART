#!/bin/bash

if [ ! -d "tmp" ]; then
	mkdir tmp
fi

cd tmp
sudo apt-get -y --force-yes install cmake

# memcached
sudo apt-get -y --force-yes install memcached libmemcached-dev

# cityhash
git clone https://github.com/google/cityhash.git
cd cityhash
./configure
make all check CXXFLAGS="-g -O3"
sudo make install
cd ..

# boost
wget https://jaist.dl.sourceforge.net/project/boost/boost/1.53.0/boost_1_53_0.zip
unzip boost_1_53_0.zip
cd boost_1_53_0
./bootstrap.sh
# ./b2 install --with-system --with-coroutine --build-type=complete --layout=versioned threading=multi
./b2 install --with-system --with-coroutine --layout=versioned threading=multi
sudo apt-get -y --force-yes install libboost-all-dev
cd ..

# paramiko
sudo apt-get -y --force-yes install python3-pip
pip3 install --upgrade pip
pip3 install paramiko

# gdown
pip3 install gdown
pip3 install --upgrade --no-cache-dir gdown

# func_timeout
pip3 install func_timeout

# matplotlib
pip3 install matplotlib

# tbb
git clone https://github.com/wjakob/tbb.git
cd tbb/build
cmake ..
make -j
sudo make install
ldconfig
cd ../..

# openjdk-8
sudo apt-get -y --force-yes install openjdk-8-jdk

cd ..
rm -rf tmp
