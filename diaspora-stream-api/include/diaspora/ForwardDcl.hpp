/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_FORWARD_DECL_HPP
#define DIASPORA_API_FORWARD_DECL_HPP

namespace diaspora {

class Archive;
struct BatchSize;
struct BufferWrapperOutputArchive;
struct BufferWrapperInputArchive;
class Consumer;
class ConsumerInterface;
class DataView;
class DataDescriptor;
class Driver;
class DriverInterface;
class Event;
class EventInterface;
struct StopEventProcessor;
class Exception;
template<typename ResultType, typename WaitFn, typename TestFn> class Future;
class Metadata;
struct NumEvents;
class Producer;
class ProducerInterface;
class SerializerInterface;
class Serializer;
class PartitionInfo;
class PartitionSelectorInterface;
class PartitionSelector;
struct PythonBindingHelper;
struct ThreadCount;
class ThreadPoolInterface;
class ThreadPool;
class TopicHandle;
class TopicHandleInterface;
class PartitionManager;
class InvalidMetadata;
class ValidatorInterface;
class Validator;

}

#endif
