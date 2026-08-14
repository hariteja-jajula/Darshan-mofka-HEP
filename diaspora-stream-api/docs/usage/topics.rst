Topics
======

Events in the Diaspora Stream API are pushed into *topics*. A topic is a distributed
collection of *partitions* to which events are appended. When creating a topic, users
have to give it a name, and optionally provide three objects.

* **Validator**: a validator is an object that validates that the metadata and data
  part comply with what is expected for the topic. Metadata are JSON documents
  by default, so for instance a validator could check that some expected fields
  are present. If the metadata part describes the data part in some way, a validator
  could check that this description is actually correct. This validation will happen
  before the event is sent to any server, resulting in an exception if the event is
  not valid. If not provided, the default validator will accept all the events it is
  presented with.

* **Partition selector**: a partition selector is an object that is given a list of
  available partitions for a topic and that will make a decision on which partition
  each event will be sent to, based on the event's metadata, or based on any other
  strategy. If not provided, the default partition selector will cycle through the
  partitions in a round robin manner.

* **Serializer**: a serializer is an object that can serialize a :code:`Metadata` object
  into a binary representation, and deserialize a binary representation back into a
  :code:`Metadata` object. If not provided, the default serializer will convert the
  :code:`Metadata` into a string representation.

.. image:: ../_static/TopicPipeline-dark.svg
   :class: only-dark

.. image:: ../_static/TopicPipeline-light.svg
   :class: only-light

Diaspora API implementations may take advantage of multithreading to parallelize and
pipeline the execution of the validator, partition selector, and serializer over many
events. These objects can be customized and parameterized. For instance, a validator
that checks the content of a JSON metadata could be provided with a list of fields it
expects to find in the metadata of each event.

.. topic:: A motivating example

   Hereafter, we will create a topic accepting events that represent collisions in a
   particle accelerator. We will require that the metadata part of such events have
   an *energy* value, represented by an unsigned integer (just so we can show
   what optimizations could be done with the API's modularity). Furthermore, let's say that
   the detector is calibrated to output energies from 0 to 99. We can create a validator that
   checks that the energy field is not only present, but that its value is also strictly lower
   than 100. If we would like to aggregate events with similar energy values into the same partition,
   we could have the partition selector make its decision based on this energy value.
   Finally, since we know that the energy value is between 0 and 99 and is the only relevant
   part of an event's metadata, we could serialize this value into a single byte (:code:`uint8_t`),
   drastically reducing the metadata size compared with a string like :code:`{"energy":42}`.


Hereafter we continue to use the "files" driver for the Diaspora Stream API, though the same
code will work with any implementation of the API.

Creating a topic
----------------

The following code snippets show how to create a topic. Such topic creation should generally
be done using the :code:`diaspora-ctl` command-line tool, however it is also possible to create
topics in C++ and in Python. Our custom validator, partition selector,
and serializer are provided using the :code:`"name:library.so"` format. This tells the
API to dynamically load the specified libraries to get access to their implementation.

.. important::

   If you get an error indicating that :code:`dlopen` has failed to find your library for
   the validator, partition selector, or serializer, make sure that :code:`LD_LIBRARY_PATH`
   contains the path to find these libraries.


.. tabs::

   .. group-tab:: diaspora-ctl

      .. literalinclude:: ../_code/energy_topic.sh
         :language: bash
         :start-after: START CREATE TOPIC
         :end-before: END CREATE TOPIC

      Configuration parameters of each objects are passed using hierarchical
      command-line options. For instance,
      :code:`--validator.x 42 --validator.y.z abc`
      will produce the configuration :code:`{ "x": 42, "y": { "z": "abc" }}`.

   .. group-tab:: C++

      .. literalinclude:: ../_code/energy_topic.cpp
         :language: cpp
         :start-after: START CREATE TOPIC
         :end-before: END CREATE TOPIC
         :dedent: 8

   .. group-tab:: Python

      .. literalinclude:: ../_code/energy_topic.py
         :language: python
         :start-after: START CREATE TOPIC
         :end-before: END CREATE TOPIC
         :dedent: 4

Let's take a look at the implementation of the validator, partition selector,
and serializer classes.

.. important::

   Validators, partition selectors, and serializers must currently be implemented
   in C++, even when used in Python.

.. literalinclude:: ../_code/energy_validator.cpp
   :language: cpp

The :code:`EnergyValidator` class inherits from :code:`diaspora::ValidatorInterface`
and provides the :code:`validate` member function. This function checks for the
presence of an :code:`energy` field of type unsigned integer and checks that
its value is less than an :code:`energy_max` value provided when creating the
validator. If validation fails, the :code:`validate` function throws an exception.

.. important::

   The :code:`DIASPORA_REGISTER_VALIDATOR` macro must be used to tell the API
   about the :code:`EnergyValidator` class. Its first argument is the name by
   which we will refer to the class in user code (*"energy_validator"*), the
   second argument is the name of the class itself (*EnergyValidator*).

.. literalinclude:: ../_code/energy_partition_selector.cpp
   :language: cpp

The :code:`EnergyPartitionSelector` is also initialized with an :code:`energy_max`
value and uses it to aggregate events into uniform "bins" of similar energy values.
It inherits from :code:`diaspora::PartitionSelectorInterface` and we call
:code:`DIASPORA_REGISTER_PARTITION_SELECTOR` to make it available to use.

.. literalinclude:: ../_code/energy_serializer.cpp
   :language: cpp

The :code:`EnergySerializer` is also initialized with an :code:`energy_max` value.
This value is used to choose an appropriate number of bytes for the raw representation
of the energy when it is serialized. :code:`EnergySerializer` inherits from
:code:`SerializerInterface` and is registered using
:code:`DIASPORA_REGISTER_SERIALIZER`.
