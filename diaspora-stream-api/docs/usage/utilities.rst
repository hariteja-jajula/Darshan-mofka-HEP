Utilities
=========

The Diaspora Stream API comes with the `diaspora-ctl` executable, which can be used
for a number of things listed in this section.

Default options via environment variable
----------------------------------------

To avoid retyping the same driver options on every invocation, set the
:code:`DIASPORA_CTL_DRIVER_OPTIONS` environment variable. Its contents are
tokenized using shell-like quoting (single and double quotes, backslash
escapes; **no** variable expansion or command substitution) and prepended to
the command-line arguments before parsing. Any option provided on the actual
command line takes precedence over the same option supplied via the
environment variable.

.. code-block:: bash

   export DIASPORA_CTL_DRIVER_OPTIONS="--driver files --driver.path /tmp/my-stream"

   # Equivalent to:
   #   diaspora-ctl topic list --driver files --driver.path /tmp/my-stream
   diaspora-ctl topic list

   # CLI overrides the env-var value of --driver.path
   diaspora-ctl topic list --driver.path /tmp/other-stream

This affects the :code:`topic create`, :code:`topic list`, and :code:`fifo`
subcommands. The :code:`forward` subcommand is configured exclusively through
its TOML file and ignores this variable.

.. note::

   Defining the variable as a plain shell string and expanding it inline (e.g.
   :code:`diaspora-ctl topic list $MY_OPTS`) is fragile across shells: in zsh,
   unquoted parameter expansion does not perform word-splitting, so the whole
   string is passed as a single argument. Setting
   :code:`DIASPORA_CTL_DRIVER_OPTIONS` lets :code:`diaspora-ctl` itself do the
   tokenization, which works identically in any shell.

Manipulating topics
-------------------

diaspora-ctl topic create
^^^^^^^^^^^^^^^^^^^^^^^^^

This command allows creating a new topic, and can be used as follows.

.. code-block:: bash

   diaspora-ctl topic create --name <topic-name> --driver <driver-name>

Or more extensively:

.. code-block:: bash

   diaspora-ctl topic create --name <topic-name> \
        --driver <driver-name> \
        --driver.<option> <value> ... \
        --driver-config <driver-config.json> \
        --topic.<option> <value> ... \
        --topic-config <topic-config.json> \
        --validator <validator-name> \
        --validator-config <validator-config.json> \
        --validator.<option> <value> ... \
        --serializer <serializer-name> \
        --serializer.<option> <value> ... \
        --serializer-config <serializer-config.json> \
        --partition-selector <partition-selector-name> \
        --partition-selector-config <partition-selector-config.json> \
        --partition-selector.<option> <value>

The only mandatory arguments are the :code:`--name` and :code:`--driver`. Options
can then be provided to each component using either :code:`--<component>-config <file.json>`
or a set of :code:`--<component>.<option> <value>` parameters.

diaspora-ctl topic list
^^^^^^^^^^^^^^^^^^^^^^^

This command lists the available topics. It is used as follows.


.. code-block:: bash

   diaspora-ctl topic list --driver <driver-name>

This will print each topic name on its own line on the standard output.
If :code:`--verbose` is provided, a JSON-formatted information about the topic is added to
each line.

Creating a FIFO daemon
----------------------

In some applications, it can be convenient for clients to interact with the
streaming engine via a file descriptor rather than linking against the Diaspora
Stream API library and using the API. The :code:`diaspora-ctl fifo` command
is here to help with that.

diaspora-ctl fifo
^^^^^^^^^^^^^^^^^

This command is used as follows.

.. code-block:: bash

   diaspora-ctl fifo --driver <driver-name> \
                     --driver.<option> <value> \
                     --driver-config <driver-config.json> \
                     --control-file <control-file>

The only mandatory arguments are the :code:`--driver` and :code:`--control-file`, as well
as any option the driver may require.

This command blocks until killed, so it is best used as a daemon put in the background.
Upon starting, the control file will be created. This file allows sending commands to
to the daemon. These commands can be of two types, producer and consumer commands,
shown hereafter.

.. code-block:: bash

   # Producer command
   echo 'path -> topic (key1=value1, key2=value2, ...)' > control-file

   # Consumer command
   echo 'path <- topic (key1=value1, key2=value2, ...)' > control-file

The only difference is the direction of the arrow.

Producer command
""""""""""""""""

A producer command will make the daemon create a FIFO with the specified `path`
and a producer instance linked to the specified `topic`. Any line of text written into
this FIFO will be passed to the producer as metadata.

Options in parenthesis may be one of the following.

* :code:`format` : :code:`raw` or :code:`json` (default: :code:`raw`). If :code:`json` is
  specified, the line is interpreted as a JSON document. Otherwise it is interpreted as a
  string. Note that because the daemon splits events on new lines, these JSON or strings
  cannot themselves embed new lines.

* :code:`batch_size` : an integer value, representing the batch size the producer must use
  (default is 128).

Note that only metadata can be written into the topic, the data part of events is always left empty.

Consumer command
""""""""""""""""

A consumer command works in a similar manner, however `the specified FIFO device must have been
created first and a process must have opened it in read mode`. The daemon will create a consumer
linked to the topic and write any received metadata into the FIFO device.

Options may again be provided in parenthesis, with currently supported options as follows.

* :code:`batch_size` : an integer value, representing the batch size the producer must use
  (default is 128).

Forwarding events between topics
---------------------------------

The :code:`diaspora-ctl forward` command runs a daemon that reads events from source topics
and forwards them to destination topics, potentially across different drivers. This is useful
for cross-driver event replication (e.g., files to Kafka).

.. code-block:: bash

   diaspora-ctl forward --config <config.toml> --logging <level>

The forwarding daemon is configured via a TOML file specifying drivers and forwarding policies.
For full documentation and example configurations, see :doc:`forwarding`.

