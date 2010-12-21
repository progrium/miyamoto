sudo /usr/local/python/bin/easy_install-2.5 eventlet==dev
wget http://download.zeromq.org/zeromq-2.0.10.tar.gz
tar -zxvf zeromq-2.0.10.tar.gz
cd zeromq-2.0.10
./configure
sudo make install
sudo /usr/local/python/bin/easy_install-2.5 pyzmq

LD_LIBRARY_PATH=/usr/local/lib