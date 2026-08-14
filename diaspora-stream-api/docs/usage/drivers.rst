Drivers
=======

This page list existing driver implementations and explains how to make your own driver.

Mofka
-----

`Mofka <https://github.com/mochi-hpc/mofka>`_ is an event-driven service for HPC based
on the `Mochi <https://wordpress.cels.anl.gov/mochi/>`_ suite of technologies. It relies
on RPC and RDMA using the Mercury library and on user-level threads thanks to the
Argobots library. To use Mofka as a backend, simply follow the README in Mofka's repository.
Thanks to its use of high-performance networks, Mofka is particularly well suited for use
in HPC applications.

Octopus
-------

`Octopus <https://github.com/diaspora-project/diaspora-stream-octopus>`_ is a driver
allowing the use of the Diaspora Stream API on top of `Apache Kafka <https://kafka.apache.org/>`_
or any engine compatible with `librdkafka <https://github.com/confluentinc/librdkafka>`_
(e.g. `Redpanda <https://www.redpanda.com/>`_). It can be used with a local deployment
of such engines, or with one deployed on Amazon MSK, using IAM access control.
To use Octopus as a backend, simply follow the README in Octopus' repository.

Files
-----

The "files" driver is built into the Diaspora Stream API library and is used throughout
this documentation. This driver organizes topics and partitions as files in a parallel
file system, with each topic represented by a directory, and each partition represented
by a subdirectory containing three files: a data file, a metadata file, and an index file.
This backend is not meant for performance. We recommend users use it for testing before
switching to an actual event-driven backend.

Writing your own
----------------

To write your own driver, navigate to
`this Github project <https://github.com/diaspora-project/diaspora-stream-template-cpp/>`_.
It is a template project, so you can click on "Use this template" and follow the instructions
in the README to develop your driver. We recommend your project name be a single word, as
initialization scripts will rename files, namespaces, etc. after the project is created.
