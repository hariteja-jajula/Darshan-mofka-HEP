Benchmarks
==========

The Diaspora Stream API repository includes two benchmark programs designed to measure
the performance of different driver implementations: a producer benchmark and a consumer
benchmark. These benchmarks are MPI-enabled, allowing for distributed testing across
multiple processes.

Both benchmarks support any driver implementation of the Diaspora Stream API, making them
useful tools for comparing performance characteristics of different backends or tuning
configuration parameters.


Producer Benchmark
------------------

The producer benchmark (:code:`diaspora-producer-benchmark`) measures the throughput
and bandwidth of producing events into a topic. It generates random metadata and data
for a specified number of events and measures the time taken to push them all into
a topic.

Command-Line Options
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   diaspora-producer-benchmark [OPTIONS]

**Required Arguments:**

* :code:`-d, --driver <driver>` - Driver to use (e.g., "files", "mofka")
* :code:`-t, --topic <topic>` - Topic name to produce events into

**Optional Arguments:**

* :code:`-c, --driver-config <file>` - Path to JSON configuration file for the driver
* :code:`-n, --num-events <N>` - Number of events to send (default: 1000)
* :code:`-s, --data-size <bytes>` - Size of the data part of each event in bytes (default: 0)
* :code:`-m, --metadata-size <bytes>` - Size of the metadata part of each event in bytes (default: 0)
* :code:`-b, --batch-size <N>` - Number of events to batch together before sending (default: 1)
* :code:`-p, --num-threads <N>` - Number of background threads for the producer to use (default: 1)
* :code:`-f, --flush-interval <N>` - Number of events to push before calling flush (default: 1000)

Example Usage
~~~~~~~~~~~~~

Basic single-process benchmark with the Files driver:

.. code-block:: bash

   diaspora-producer-benchmark \
       --driver files \
       --driver-config driver.json \
       --topic benchmark-topic \
       --num-events 10000 \
       --data-size 1024 \
       --metadata-size 256 \
       --batch-size 100 \
       --num-threads 4 \
       --flush-interval 1000

Multi-process benchmark using MPI:

.. code-block:: bash

   mpirun -n 4 diaspora-producer-benchmark \
       --driver files \
       --driver-config driver.json \
       --topic benchmark-topic \
       --num-events 100000 \
       --data-size 4096 \
       --metadata-size 128 \
       --batch-size 128 \
       --num-threads 8

Output
~~~~~~

The benchmark outputs the following metrics (printed by rank 0):

* **Total time**: Time elapsed for all events to be produced (in seconds)
* **Total events**: Total number of events produced
* **Throughput**: Number of events produced per second
* **Bandwidth**: Total data throughput in MB/s (includes both metadata and data)

Example output:

.. code-block:: text

   --- Benchmark Options ---
   Driver: files
   Driver config file: driver.json
   Topic: benchmark-topic
   Number of events: 10000
   Data size: 1024
   Metadata size: 256
   Batch size: 100
   Number of threads: 4
   Flush interval: 1000

   --- Benchmark Results ---
   Total time: 2.45 s
   Total events: 10000
   Throughput: 4081.63 events/s
   Bandwidth: 5.10 MB/s


Consumer Benchmark
------------------

The consumer benchmark (:code:`diaspora-consumer-benchmark`) measures the throughput
of consuming events from a topic. It supports options to control data selectivity,
allowing tests of scenarios where consumers only request partial data from events.

When run with multiple MPI processes, the benchmark automatically distributes partitions
across processes in a round-robin manner, with each process consuming from a subset
of the topic's partitions.

Command-Line Options
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   diaspora-consumer-benchmark [OPTIONS]

**Required Arguments:**

* :code:`-d, --driver <driver>` - Driver to use (e.g., "files", "mofka")
* :code:`-t, --topic <topic>` - Topic name to consume events from

**Optional Arguments:**

* :code:`-c, --driver-config <file>` - Path to JSON configuration file for the driver
* :code:`-n, --num-events <N>` - Number of events to receive (default: 1000)
* :code:`-p, --num-threads <N>` - Number of background threads for the consumer to use (default: 1)
* :code:`-i, --data-interest <ratio>` - Proportion of events for which to pull data, between 0 and 1 (default: 1)
* :code:`-s, --data-selectivity <ratio>` - Proportion of data to read from each event, between 0 and 1 (default: 1)

The :code:`--data-interest` parameter controls the probability that the consumer will
request the data part of an event (as opposed to just the metadata). The
:code:`--data-selectivity` parameter controls what fraction of the data to request
for events where data is requested. These parameters allow benchmarking scenarios
where consumers are selective about which data they retrieve.

Example Usage
~~~~~~~~~~~~~

Basic consumer benchmark:

.. code-block:: bash

   diaspora-consumer-benchmark \
       --driver files \
       --driver-config driver.json \
       --topic benchmark-topic \
       --num-events 10000 \
       --num-threads 4

Consumer benchmark with selective data retrieval:

.. code-block:: bash

   diaspora-consumer-benchmark \
       --driver files \
       --driver-config driver.json \
       --topic benchmark-topic \
       --num-events 10000 \
       --num-threads 4 \
       --data-interest 0.5 \
       --data-selectivity 0.25

This example will request data for only 50% of events, and when data is requested,
only 25% of the available data will be retrieved.

Multi-process consumer benchmark:

.. code-block:: bash

   mpirun -n 4 diaspora-consumer-benchmark \
       --driver files \
       --driver-config driver.json \
       --topic benchmark-topic \
       --num-events 100000 \
       --num-threads 8

Output
~~~~~~

The benchmark outputs the following metrics (printed by rank 0):

* **Total time**: Time elapsed to consume all events (in seconds)
* **Total events**: Total number of events consumed
* **Throughput**: Number of events consumed per second

Example output:

.. code-block:: text

   --- Benchmark Options ---
   Driver: files
   Driver config file: driver.json
   Topic: benchmark-topic
   Number of events: 10000
   Number of threads: 4
   Data interest: 1.0
   Data selectivity: 1.0

   --- Benchmark Results ---
   Total time: 1.85 s
   Total events: 10000 s
   Throughput: 5405.41 events/s


Running End-to-End Benchmarks
------------------------------

To perform a complete end-to-end benchmark, you can run both the producer and consumer
benchmarks in sequence or in parallel.

Sequential Approach
~~~~~~~~~~~~~~~~~~~

First, create and populate a topic with the producer benchmark:

.. code-block:: bash

   # Create the topic
   diaspora-ctl topic create \
       --driver files \
       --driver.root_path ./benchmark-data \
       --name benchmark-topic \
       --topic.num_partitions 4

   # Run producer benchmark
   mpirun -n 2 diaspora-producer-benchmark \
       --driver files \
       --driver-config driver.json \
       --topic benchmark-topic \
       --num-events 100000 \
       --data-size 2048 \
       --metadata-size 128 \
       --batch-size 128 \
       --num-threads 4

   # Run consumer benchmark
   mpirun -n 2 diaspora-consumer-benchmark \
       --driver files \
       --driver-config driver.json \
       --topic benchmark-topic \
       --num-events 100000 \
       --num-threads 4

Parallel Approach
~~~~~~~~~~~~~~~~~

For real-time producer/consumer benchmarks, run both programs simultaneously in separate
terminals or using job schedulers:

.. code-block:: bash

   # Terminal 1: Start consumer first
   mpirun -n 2 diaspora-consumer-benchmark \
       --driver files \
       --driver-config driver.json \
       --topic benchmark-topic \
       --num-events 100000 \
       --num-threads 4

   # Terminal 2: Start producer
   mpirun -n 2 diaspora-producer-benchmark \
       --driver files \
       --driver-config driver.json \
       --topic benchmark-topic \
       --num-events 100000 \
       --data-size 2048 \
       --metadata-size 128 \
       --batch-size 128 \
       --num-threads 4

This approach measures the latency and throughput characteristics when producers and
consumers operate concurrently.


Performance Tuning Recommendations
-----------------------------------

Batch Size
~~~~~~~~~~

The batch size has a significant impact on performance. Larger batch sizes reduce
the number of operations but increase latency. Start with batch sizes around 64-256
and adjust based on your workload characteristics.

Thread Pool Size
~~~~~~~~~~~~~~~~

The number of threads controls the parallelism of validation, serialization, and
partition selection operations for producers, and deserialization for consumers.
A good starting point is to use 4-8 threads, but optimal values depend on the
complexity of your custom components and available CPU cores.

Flush Interval
~~~~~~~~~~~~~~

For the producer, the flush interval controls how often batches are forced to be sent.
Smaller intervals reduce latency but may hurt throughput. Larger intervals improve
throughput but increase the risk of data loss if the producer crashes.

Event Size
~~~~~~~~~~

When testing with synthetic data, vary the data size to match your application's
expected event sizes. Small events (< 1 KB) test metadata handling efficiency,
while large events (> 100 KB) test data transfer capabilities.

MPI Configuration
~~~~~~~~~~~~~~~~~

When running multi-process benchmarks, ensure that:

* The number of MPI processes evenly divides the number of partitions for best load balancing
* For consumer benchmarks, having more processes than partitions is acceptable (some processes will be idle)
* For producer benchmarks, all processes will produce independently into the topic

Comparing Drivers
~~~~~~~~~~~~~~~~~

To compare different driver implementations, run the same benchmark configuration
with each driver and compare the throughput and bandwidth metrics. Make sure to:

* Use the same topic configuration (number of partitions, etc.)
* Use the same event sizes and counts
* Run multiple iterations to account for variance
* Clear any caches between runs if applicable
