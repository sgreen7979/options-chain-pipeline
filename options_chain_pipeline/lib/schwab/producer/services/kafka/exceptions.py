class ConsumerAlreadySetError(RuntimeError):
    """
    Raised when a KafkaService tries to initialize a
    consumer when one was already set
    """


class ProducerAlreadySetError(RuntimeError):
    """
    Raised when a KafkaService tries to initialize a
    producer when one was already set
    """
