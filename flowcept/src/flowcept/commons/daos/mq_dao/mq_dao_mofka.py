import uuid
from typing import Callable

import msgpack
from time import time
import json

import mochi.mofka.client as mofka
from mochi.mofka.client import ThreadPool, AdaptiveBatchSize

from flowcept.commons.daos.mq_dao.mq_dao_base import MQDao
from flowcept.configs import MQ_SETTINGS, MQ_CHANNEL


class MQDaoMofka(MQDao):
    """Main class to communicate with Mofka."""

    _driver = mofka.MofkaDriver(group_file=MQ_SETTINGS.get("group_file",None))
    _topic = _driver.open_topic(MQ_SETTINGS["channel"])

    def __init__(self, adapter_settings=None, with_producer=True):
        super().__init__(adapter_settings=adapter_settings)
        self.producer = None
        if with_producer:
            print("Starting producer")
            self.producer = MQDaoMofka._topic.producer(
                "p" + MQ_CHANNEL,
                batch_size=mofka.AdaptiveBatchSize,
                ordering=mofka.Ordering.Strict,
            )

    def subscribe(self):
        """Subscribe to Mofka topic.

        If MQ_SETTINGS carries a non-empty ``targets`` list (partition indices),
        pin this consumer to exactly those partitions so N consumers can drain
        disjoint shards of the topic in parallel. Absent/empty ``targets`` keeps
        the default all-partitions behavior (back-compatible).
        """
        kwargs = {
            "name": MQ_CHANNEL + str(uuid.uuid4()),
            "batch_size": AdaptiveBatchSize,
        }
        targets = MQ_SETTINGS.get("targets")
        if targets:
            kwargs["targets"] = [int(t) for t in targets]
        self.consumer = MQDaoMofka._topic.consumer(**kwargs)

    def message_listener(self, message_handler: Callable):
        """Mofka's Message listener.

        F1 v2 (added 2026-06-23): the darshan-mofka producer may pack N
        events into one mofka message with envelope shape:
            {"__batch__": 1, "n": N, "events": [ev1, ..., evN]}
        when DARSHAN_MOFKA_PACKED_BATCH=1 is set on the producer side.
        Unpack and dispatch event-by-event so DocumentInserter sees the
        same per-event stream regardless of producer packing.
        Backward compatible: non-batch messages (no "__batch__" key) hit
        the else branch and are forwarded unchanged.
        """
        try:
            while True:
                event = self.consumer.pull().wait()
                message = event.metadata
                self.logger.debug(f"Received message: {message}")
                if isinstance(message, dict) and message.get("__batch__"):
                    events = message.get("events", [])
                    self.logger.debug(f"Unpacking F1 v2 batch n={len(events)}")
                    stop = False
                    for evt in events:
                        if not message_handler(evt):
                            stop = True
                            break
                    if stop:
                        break
                else:
                    if not message_handler(message):
                        break
        except Exception as e:
            self.logger.exception(e)
        finally:
            pass

    def send_message(self, message: dict, channel=MQ_CHANNEL, serializer=msgpack.dumps):
        """Send a single message to Mofka."""
        self.producer.push(metadata=message)  # using metadata to send data
        self.producer.flush()

    def _send_message_timed(self, message: dict, channel=MQ_CHANNEL, serializer=msgpack.dumps):
        t1 = time()
        self.send_message(message, channel, serializer)
        t2 = time()
        self._flush_events.append(["single", t1, t2, t2 - t1, len(str(message).encode())])

    def _bulk_publish(self, buffer, channel=MQ_CHANNEL, serializer=msgpack.dumps):
        try:
            # self.logger.debug(f"Going to send Message:\n\t[BEGIN_MSG]{buffer}\n[END_MSG]\t")
            for m in buffer:
                self.producer.push(metadata=m)

        except Exception as e:
            self.logger.exception(e)
            self.logger.error("Some messages couldn't be flushed! Check the messages' contents!")
            self.logger.error(f"Message that caused error: {buffer}")
        try:
            self.producer.flush()
            # self.logger.info(f"Flushed {len(buffer)} msgs to MQ!")
        except Exception as e:
            self.logger.exception(e)

    def _bulk_publish_timed(self, buffer, channel=MQ_CHANNEL, serializer=msgpack.dumps):
        total = 0
        try:
            # self.logger.debug(f"Going to send Message:\n\t[BEGIN_MSG]{buffer}\n[END_MSG]\t")

            for m in buffer:
                self.producer.push(metadata=m)
                total += len(str(m).encode())

        except Exception as e:
            self.logger.exception(e)
            self.logger.error("Some messages couldn't be flushed! Check the messages' contents!")
            self.logger.error(f"Message that caused error: {buffer}")
        try:
            t1 = time()
            self.producer.flush()
            t2 = time()
            self._flush_events.append(["bulk", t1, t2, t2 - t1, total])
            # self.logger.info(f"Flushed {len(buffer)} msgs to MQ!")
        except Exception as e:
            self.logger.exception(e)

    def liveness_test(self):
        """Test Mofka Liveness."""
        return True

    def unsubscribe(self):
        """Stop pulling from the Mofka topic.

        Mofka does not expose a topic-level unsubscribe like Kafka; the
        subscription lifecycle is bound to the consumer object. Releasing
        the reference here lets the underlying pymofka_client consumer
        be cleaned up by GC.
        """
        self.consumer = None
