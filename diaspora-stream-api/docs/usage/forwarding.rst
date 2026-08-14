Forwarding
==========

The :code:`diaspora-ctl forward` command runs a long-lived daemon that reads events
from one or more source topics and forwards them to destination topics. This enables
cross-driver event forwarding (e.g., from a local files-based topic to a remote
Kafka topic) without writing any application code.

Each forwarding policy runs in its own thread with a dedicated consumer and producer,
isolating failures and avoiding shared-state issues.

Basic usage
-----------

The forwarding daemon is configured with a YAML file specifying the drivers to use
and the forwarding policies to apply.

.. code-block:: bash

   diaspora-ctl forward --config <config.yaml>

Or with additional options:

.. code-block:: bash

   diaspora-ctl forward --config <config.yaml> --logging <level>

The :code:`--config` argument is mandatory and points to a YAML configuration file.
The :code:`--logging` argument is optional and can be one of :code:`trace`, :code:`debug`,
:code:`info`, :code:`warn`, :code:`error`, :code:`critical`, or :code:`off`
(default: :code:`info`).

The daemon runs continuously until it receives a :code:`SIGINT` or :code:`SIGTERM` signal
(e.g., via :code:`Ctrl+C` or :code:`kill`). On shutdown, each worker thread flushes its
producer before terminating, ensuring no events are lost.

Configuration file format
-------------------------

The configuration file uses `YAML <https://yaml.org>`_ format and consists of two sections:
a :code:`drivers` mapping defining the streaming backends, and a :code:`forward` list
defining the forwarding policies.

Defining drivers
^^^^^^^^^^^^^^^^

Each driver is defined as a sub-mapping under :code:`drivers`. The mapping key becomes the
driver's name (used in forwarding policies), and the :code:`type` field specifies which
Diaspora driver implementation to load. All other fields are passed as metadata to the driver.

.. code-block:: yaml

   drivers:
     local:
       type: files
       options:
         root_path: /data/local
     remote:
       type: kafka
       options:
         bootstrap: "kafka:9092"

This defines two drivers: :code:`local` (using the files driver) and :code:`remote`
(using a Kafka driver). The :code:`options` mapping is converted to JSON and passed
as metadata when creating the driver.

Defining forwarding policies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each entry in the :code:`forward` list defines a policy that copies events from a source
topic to a destination topic. The :code:`from` and :code:`to` fields use the format
:code:`driver_name/topic_name`.

.. code-block:: yaml

   forward:
     - from: local/source-topic
       to:   remote/dest-topic

Multiple policies can be defined to forward from multiple source topics simultaneously.
Each policy runs in its own thread.

Example configurations
----------------------

Local forwarding
^^^^^^^^^^^^^^^^

The simplest use case forwards events between two topics on the same driver.
This can be useful for replicating data or for feeding derived topics.

.. literalinclude:: ../_code/forward_local.yaml
   :language: yaml
   :start-after: START BASIC CONFIG
   :end-before: END BASIC CONFIG

.. code-block:: bash

   diaspora-ctl forward --config forward_local.yaml

Cross-driver forwarding
^^^^^^^^^^^^^^^^^^^^^^^^

A more common use case is forwarding events between different drivers, for instance
from a local files-based staging area to a remote Kafka cluster.

.. literalinclude:: ../_code/forward_cross_driver.yaml
   :language: yaml
   :start-after: START CROSS DRIVER CONFIG
   :end-before: END CROSS DRIVER CONFIG

This configuration defines two forwarding policies that run concurrently, each in its
own thread.

Custom data selection and allocation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For advanced use cases, you can specify custom :code:`DataSelector` and
:code:`DataAllocator` plugins that control which parts of an event's data are read
and how memory is allocated for the data on the consumer side. These are loaded
dynamically via :code:`dlopen` and specified using the format
:code:`library.so:factory_function`.

.. literalinclude:: ../_code/forward_plugins.yaml
   :language: yaml
   :start-after: START PLUGIN CONFIG
   :end-before: END PLUGIN CONFIG

The factory functions must be :code:`extern "C"` functions returning a
:code:`diaspora::DataSelector` or :code:`diaspora::DataAllocator` respectively:

.. code-block:: cpp

   #include <diaspora/DataSelector.hpp>
   #include <diaspora/DataAllocator.hpp>

   extern "C" diaspora::DataSelector my_selector() {
       return [](const diaspora::Metadata& metadata,
                 const diaspora::DataDescriptor& descriptor) {
           // Return the full descriptor to select all data,
           // or DataDescriptor{} to skip data entirely
           return descriptor;
       };
   }

   extern "C" diaspora::DataAllocator my_allocator() {
       return [](const diaspora::Metadata& metadata,
                 const diaspora::DataDescriptor& descriptor) {
           // Allocate memory for the incoming data
           auto* buffer = new char[descriptor.size()];
           return diaspora::DataView{
               buffer, descriptor.size(), buffer,
               [](void* ctx) { delete[] static_cast<char*>(ctx); }
           };
       };
   }

How it works
------------

The forwarding daemon operates as follows:

1. **Startup**: The YAML configuration is parsed and validated. All drivers are
   instantiated and all source/destination topics are opened. If any driver or topic
   cannot be created, the daemon exits immediately with an error.

2. **Worker threads**: For each :code:`forward` policy, a dedicated thread is
   spawned containing a consumer (reading from the source topic) and a producer
   (writing to the destination topic).

3. **Event loop**: Each worker thread continuously pulls events from its consumer
   with a 200ms timeout (to allow responsive shutdown checks). Successfully pulled
   events are pushed to the destination producer and then acknowledged on the source.

4. **Graceful shutdown**: Upon receiving :code:`SIGINT` or :code:`SIGTERM`, each worker
   thread exits its loop, flushes its producer (with a 5-second timeout), and
   terminates. The daemon waits for all threads to join before exiting.

.. important::

   The forwarding daemon requires that source and destination topics already exist
   before it is started. Use :code:`diaspora-ctl topic create` to create them first.

.. important::

   If a transient error occurs during event pulling or pushing (e.g., a temporary
   network issue), the worker thread logs the error and retries after a short delay.
   The daemon does not exit on transient errors.
