Getting started
===============

Without going into details that will be covered later, the Diaspora Stream API
essentially borrows the terminology of event-driven systems like Kafka: `events`
are produced into and consumed from `topics`, which are themselves split into
a number `partitions`. Each partition represents an append-only log.

In this documentation, we will use the **files** driver that ships with the
Diaspora Stream API. Other implementations are discussed in a later section.

Creating a topic and a partition
--------------------------------

Short of writing C++ or Python code, the easiest way to create a new topic
is to use the :code:`diaspora-ctl` command line tool as follows.

.. code-block:: bash

   diaspora-ctl topic create --name my_topic \
                             --driver files \
                             --driver.root_path /tmp/diaspora-data \
                             --topic.num_partitions 1

This command takes **--name** and **--driver** as mandatory arguments. Driver options
can be supplied using the :code:`--driver.<option-name>` syntax (e.g. here the :code:`root_path`
option, which specifies where to store the data in the file system). Similarly, topic
options can be supplied using :code:`--topic.<option-name>`.

.. note::

   If many options must be passed to the driver (resp. the topic), a
   :code:`--driver-config <filename.json>` may be supplied (resp. :code:`--topic-config`),
   which provides the options in the form of a JSON file. :code:`diaspora-ctl` will merge
   options coming from a JSON file with options coming from the command line, with the
   latter overwriting the former.

We can check that the topic has been created successfully by listing the available
topic as follows.

.. code-block:: bash

   diaspora-ctl topic list --driver files --driver.root_path /tmp/diaspora-data


Using the Diaspora Stream API library
-------------------------------------

The Diaspora Stream API can be used in C++ or in Python (if built with Python support).
The following *CMakeLists.txt* file shows how to link a C++ application against the
Diaspora Stream API library in CMake.


.. literalinclude:: ../_code/CMakeLists.txt
   :language: cmake
   :end-before: CUSTOM TOPIC OBJECTS


In Python, the API is located in the :code:`diaspora_stream.api` module.
The sections hereafter show how to use both the C++ and Python interface to produce
and consume events.


Simple producer application
---------------------------

The following code exemplifies a producer.
We first create a :code:`Driver` object, passing it some options including the :code:`root_path`
required by the "files" driver. The first argument passed to :code:`Driver::New` (or the Driver's
constructor in Python), "files", tells it to load the Files driver implementation. This
implementation is built into the library, so it does not need to be dynamically loaded.
In general, passing "<name>" as the argument  will make the API automatically search for
*lib<name>.so* in :code:`LD_LIBRARY_PATH`. If the implementation is located in a different
library, the caller may use the syntax :code:`"<name>:lib<the-library-name>.so"` .

We then open the topic we have created, using :code:`driver.openTopic()`
(:code:`driver.open_topic()` in Python), which gives us a :code:`TopicHandle`
to interact with the topic.

We create a :code:`Producer` using :code:`topic.producer()`, and we use
it in a loop to create 100 events with their :code:`Metadata` and :code:`DataView`
parts (we always send the same metadata here and we don't provide any data).
In Python, the metadata part can be either a :code:`str` representing a valid JSON document,
or a :code:`dict` convertible to JSON, and the data part can be anything that satisfies
the buffer protocol, or a list of objects that each satisfy the buffer protocol.

The :code:`push()` function is non-blocking. It returns a future object that callers
can wait on to obtain the event ID after the event has been stored. The call to :code:`wait()`
in the code below is however commented: a better practice consists of periodically flushing
the producer by calling :code:`producer.flush()`, or at least wait on futures as late as possible
so as to overlap the sending of the event with actual work from the application.

:code:`producer.flush()` is also a non-blocking function. It returns a future that can be awaited.

All the Future objects used in the Diaspora Stream API have a :code:`wait()` function taking
a timeout value in milliseconds. This timeout **is not** the timeout of the underlying operation
itself, it is the duration after which the :code:`wait()` call should unblock even if the operation
has not completed. In C++, :code:`wait()` returns an :code:`std::optional`. If the :code:`wait`
times out, this optional will not be set. In Python, :code:`wait()` will return :code:`None` if it
has timed out. A :code:`wait()` call that has timed out can be retried. Finally, passing a negative
value to :code:`wait()` will make the code block until the result is available (no timeout).

.. important::

   While the producer will make a copy of the metadata part of an event,
   it will NOT make a copy of its data part. It is the caller's responsibility
   to ensure that the data part remains alive and is not modified until the
   corresponding call to :code:`wait()` or :code:`flush()`. Forgetting this is
   the number one cause of data corruption or crashes.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/producer.cpp
         :language: cpp

   .. group-tab:: Python

      .. literalinclude:: ../_code/producer.py
         :language: python


Simple consumer application
---------------------------

The following code shows how to create a consumer and use it to consume the events.

The consumer object is created with a name. This is for the underlying backend
to keep track of the last event that was acknowledged by each application.
In case of a crash of the application, it will be able to restart from the
last acknowledged event. This acknowledgement is done using the
:code:`Event`'s :code:`acknowledge()` function, which in the example bellow
is called every 10 events.

:code:`consumer.pull()` is a non-blocking function returning a :code:`Future`.
Waiting for this future with :code:`.wait()` returns an optional :code:`Event`
object from which we can retrieve an event ID as well as the event's metadata
and data.

As it is, the data associated with an event will not be pulled automatically
by the consumer, contrary to the event's metadata. Further in this documentation
you will learn how to pull this data, or part of it.

.. tabs::

   .. group-tab:: C++

      .. literalinclude:: ../_code/consumer.cpp
         :language: cpp

   .. group-tab:: Python

      .. literalinclude:: ../_code/consumer.py
         :language: python

