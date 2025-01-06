charmpandas
============

A python library for distributed data analytics

Installation
------------

1. Install Charm++ non-SMP build

2. Install pyccs. You can find the instructions here - https://github.com/charmplusplus/PyCCS

3. Install arrow. The instructions are here - https://arrow.apache.org/install/

4. Install charmpandas::

    git clone https://github.com/adityapb/charmpandas.git
    cd charmpandas/src
    make -j6
    cd ..
    python setup.py install

Running an example
------------------

1. First use the ``examples/datagen.py`` script to generate data for the example.::

    python datagen.py

2. Edit the ``examples/Demo.ipynb`` notebook to point to the correct parquet files

3. Run the server from the ``charmpandas/src`` directory::
    
    ./charmrun +p4 ./server.out +balancer MetisLB +LBDebug 3 ++server ++server-port 1234

4. Then run the Demo notebook
