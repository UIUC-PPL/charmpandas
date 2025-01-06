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
