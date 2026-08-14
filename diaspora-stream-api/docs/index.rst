Welcome to the Diaspora Stream API
==================================

The Diaspora Stream API is a C++ and Python library defining an API
for streaming engines for high-performance computing platforms and
applications. It is developed in the context of the
`Diaspora project <https://diaspora-project.github.io/>`_.

One of the particularities of this API is that it splits events into two parts:
a **data** part, referencing potentially large, raw data, and a **metadata** part,
which consists of structured information about the data (expressed in JSON).
This separation allows the underlying implementations to optimize independently
the data and metadata transfers, for instance by relying on zero-copy mechanisms,
RDMA, or batching. This interface is also often more adapted to HPC applications,
which manipulate large datasets and their metadata.

Three implementations of the Diaspora Stream API currently exist.

* `Mofka <https://mofka.readthedocs.io>`_, a streaming framework based on
  `Mochi <https://wordpress.cels.anl.gov/mochi/>`_, a set of tools and methodologies
  for building highly-efficient HPC data services. Mofka benefits from high-speed HPC
  networks through the `Mercury <https://mercury-hpc.github.io/>`_ RPC and RDMA library
  and a high level of on-node concurrency using
  `Argobots <https://www.argobots.org/>`_.
* `Octopus <https://github.com/diaspora-project/diaspora-stream-octopus>`_, an
  implementation of the API using librdkafka and meant to interact with a Kafka
  deployment on AWS, using Globus Auth for authentication.
* `Files <https://github.com/diaspora-project/diaspora-stream-api/tree/main/src/files-driver>`_
  can also be used to emulate a streaming engine on top of a file system, using
  append-only files as streams. This backend is built into the Diaspora Stream API
  project and can also be used to test streaming application before moving to an
  actual streaming engine.

The Diaspora Stream API repository also provides a number of utilities, such as
`diaspora-ctl`, a command-line tool with a number of options to interact with
a streaming engine. A number of benchmarks are also provided to allow users to
compare various implementations of the API.

One of the goals of the Diaspora Stream API is to foster research in streaming for
HPC. Hence, anyone is welcome to provide an implementation of this API and compare it
with existing ones. To help with this, we provide a `template Github repository
<https://github.com/diaspora-project/diaspora-stream-template-cpp>`_ to get started.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   usage/installation.rst
   usage/quickstart.rst
   usage/topics.rst
   usage/producer.rst
   usage/consumer.rst
   usage/deployment.rst
   usage/restarting.rst
   usage/drivers.rst
   usage/utilities.rst
   usage/forwarding.rst
   usage/benchmarks.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
