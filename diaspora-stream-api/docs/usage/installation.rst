Installation
============

Diaspora Stream API can be built using `Spack <https://spack.io/>`_.
It is therefore assumed that the user has installed and setup Spack.

The *diaspora-spack-packages* repository needs to be added to spack as follows.

.. code-block:: bash

   git clone https://github.com/diaspora-project/diaspora-spack-packages.git
   spack repo add diaspora-spack-packages/spack_repo/diaspora

We recommend that you update your clone of *diaspora-spack-packages* from time to time
to get the latest versions of all the dependencies.

You can then install the Diaspora Stream API as follows.

.. code-block:: bash

   spack install diaspora-stream-api


Installing in a Spack environment
---------------------------------

Alternatively, you can isolate your installation in a Spack environment as follows
(replace *myenv* with the name you want to give the environment).

.. code-block:: bash

   spack env create myenv
   spack env activate myenv
   spack repo add https://github.com/diaspora-project/diaspora-spack-packages.git '$env/repos'
   spack add diaspora-stream-api
   spack install

.. note::

   Here we pass the URL to the diaspora-spack-packages repository to :code:`spack repo add`,
   to let Spack clone it. We could also have used the same method as above, cloning it manually
   and calling :code:`spack repo add` with the local path. Letting Spack clone it into the
   environment is a little better for isolation.
   Note the quotes around :code:`$env/repos`, this is because we don't want :code:`$env` to
   be expanded by the shell, we want spack itself to replace it with the environment's path.

It is generally recommended to use Spack environments to better manage package isolation.
Adding *diaspora-spack-packages* this way also ensures that it is added only to the environment
and not globally.

Diaspora Stream API will be built with Python support by default. This Python support provides
a Python wrapper around the C++ API. Should you wish to disable it, simply install
`diaspora-stream-api ~python`.

Another option (enabled by default) is :code:`+benchmarks`, which will build generic benchmark
codes to test implementations of the API.

The :code:`+tests` option is also enabled by default and will make Spack install the unit tests.
Diaspora Stream API ships with some scripts that allow running these unit tests against a new
implementation of the API, to help implementers ensure that their implementation at least
pass the Diaspora Stream API's test suite.


Installing with pip
-------------------

While Spack is the recommended method to build and install the Diaspora Stream API, it is
also possible to build it using `pip`, as follows (e.g. inside a virtual environment or a
conda environment).

.. code-block:: bash

   # Create and activate your virtual environment
   python -m venv .venv
   source .venv/bin/activate

   # Install the Diaspora Stream API (main branch)
   pip install git+https://github.com/diaspora-project/diaspora-stream-api

