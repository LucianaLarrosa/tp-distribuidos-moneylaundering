import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchangeDirect,
    MessageMiddlewareExchangeFanout,
    MessageMiddlewareExchangeTopic,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)


class MessageMiddlewareRabbitMQBase:

    def start_consuming(self, on_message_callback):
        """
        Starts consuming messages from the queue or exchange and invokes the callback for each message received.
        The callback receives the message body, an ack function, and a nack function as parameters.
        If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
        If an internal error occurs that cannot be resolved, it raises MessageMiddlewareMessageError.
        """

        def ack_nack_callback_adapter(ch, method, properties, body):
            on_message_callback(
                body,
                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                lambda: ch.basic_nack(delivery_tag=method.delivery_tag),
            )

        try:
            self.channel.basic_consume(
                queue=self.queue_name, on_message_callback=ack_nack_callback_adapter
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(
                f"An internal error occurred while consuming: {e}"
            )

    def stop_consuming(self):
        """
        Stops consuming messages from the queue or exchange.
        If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
        """
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )

    def stop_consuming_threadsafe(self):
        """
        Stops consuming messages from the queue or exchange in a thread-safe manner.
        If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
        """
        try:
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )

    def close(self):
        """
        Closes the connection to the RabbitMQ server.
        If an internal error occurs that cannot be resolved, it raises MessageMiddlewareCloseError.
        """
        try:
            if not self.connection.is_open:
                return

            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(
                f"An error occurred while closing the connection: {e}"
            )


class MessageMiddlewareQueueRabbitMQ(
    MessageMiddlewareRabbitMQBase, MessageMiddlewareQueue
):

    def __init__(self, host, queue_name):
        """
        Initializes the connection to the RabbitMQ server and declares the queue.
        """
        self.queue_name = queue_name

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, heartbeat=0)
        )
        try:
            self.channel = self.connection.channel()
            self.channel.confirm_delivery()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
        except Exception:
            self.close()

    def send(self, message):
        """
        Sends a message to the queue.
        If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
        If an internal error occurs that cannot be resolved, it raises MessageMiddlewareMessageError.
        """
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(
                f"An internal error occurred while sending: {e}"
            )


class MessageMiddlewareExchangeDirectRabbitMQ(
    MessageMiddlewareRabbitMQBase, MessageMiddlewareExchangeDirect
):

    def __init__(self, host, exchange_name, routing_keys, queue_name=None):
        """
        Initializes the connection to the RabbitMQ server, declares the exchange and binds a queue to it.
        If queue_name is provided, consumers share a named durable queue (broker round-robins messages → work distribution).
        Otherwise each consumer gets its own exclusive queue (broadcast).
        """
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, heartbeat=0)
        )
        try:
            self.channel = self.connection.channel()
            self.channel.confirm_delivery()
            self.channel.exchange_declare(
                exchange=self.exchange_name, exchange_type="direct", durable=True
            )

            if queue_name is None:
                result = self.channel.queue_declare(queue="", exclusive=True)
                self.queue_name = result.method.queue
            else:
                self.channel.queue_declare(queue=queue_name, durable=True)
                self.queue_name = queue_name
            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key,
                )
        except Exception:
            self.close()

    def send(self, message, routing_key=None):
        """
        Sends a message to the exchange.
        If routing_key is provided, publishes only to that key (topic/sharder mode).
        Otherwise publishes to all routing_keys declared at init time.
        """
        keys = [routing_key] if routing_key is not None else self.routing_keys
        try:
            for key in keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=key,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent
                    ),
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(
                f"An internal error occurred while sending: {e}"
            )


class MessageMiddlewareExchangeFanoutRabbitMQ(
    MessageMiddlewareRabbitMQBase, MessageMiddlewareExchangeFanout
):

    def __init__(self, host, exchange_name):
        """Initializes the connection to the RabbitMQ server, declares the fanout exchange
        and binds a temporary queue to the exchange."""
        self.exchange_name = exchange_name

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, heartbeat=0)
        )
        try:
            self.channel = self.connection.channel()
            self.channel.confirm_delivery()
            self.channel.exchange_declare(
                exchange=self.exchange_name, exchange_type="fanout", durable=True
            )

            result = self.channel.queue_declare(queue="", exclusive=True)
            self.queue_name = result.method.queue
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
            )
        except Exception:
            self.close()

    def send(self, message):
        """Sends a message to the fanout exchange, which will be delivered to all bound queues.
        If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
        If an internal error occurs that cannot be resolved, it raises MessageMiddlewareMessageError.
        """
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key="",
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(
                f"An internal error occurred while sending: {e}"
            )


class MessageMiddlewareExchangeTopicRabbitMQ(
    MessageMiddlewareRabbitMQBase, MessageMiddlewareExchangeTopic
):

    def __init__(self, host, exchange_name, binding_patterns, queue_name=None):
        """
        binding_patterns: list of routing key patterns with wildcard support.
          '*' matches exactly one word, '#' matches zero or more words.
          e.g. ["stock.#", "*.usd.*"]
        queue_name: if provided, consumers share a named durable queue and the
          broker round-robins messages between them (work distribution). If
          None, each consumer gets its own exclusive queue (broadcast).
        """
        self.exchange_name = exchange_name
        self.binding_patterns = binding_patterns

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, heartbeat=0)
        )
        try:
            self.channel = self.connection.channel()
            self.channel.confirm_delivery()
            self.channel.exchange_declare(
                exchange=self.exchange_name, exchange_type="topic", durable=True
            )

            if queue_name is None:
                result = self.channel.queue_declare(queue="", exclusive=True)
                self.queue_name = result.method.queue
            else:
                self.channel.queue_declare(queue=queue_name, durable=True)
                self.queue_name = queue_name

            for pattern in self.binding_patterns:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=pattern,
                )
        except Exception:
            self.close()

    def send(self, message, routing_key):
        """
        Sends a message to the topic exchange with the given routing key.
        If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
        If an internal error occurs that cannot be resolved, it raises MessageMiddlewareMessageError.
        """
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )
        except Exception as e:
            self.close()
            raise MessageMiddlewareMessageError(
                f"An internal error occurred while sending: {e}"
            )
