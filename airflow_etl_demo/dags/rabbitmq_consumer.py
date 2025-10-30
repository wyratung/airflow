"""
RabbitMQ Consumer Module - Consume messages from RabbitMQ
"""

import pika
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime


class RabbitMQConsumer:
    """
    RabbitMQ Consumer để nhận messages từ queue
    """
    
    def __init__(self, host: str = 'localhost', port: int = 5672,
                 username: str = 'guest', password: str = 'guest',
                 exchange: str = 'xml.exchange', 
                 queue: str = 'xml.queue',
                 routing_key: str = 'xml.key'):
        """
        Initialize RabbitMQ consumer
        
        Args:
            host: RabbitMQ host
            port: RabbitMQ port
            username: RabbitMQ username
            password: RabbitMQ password
            exchange: Exchange name
            queue: Queue name
            routing_key: Routing key
        """
        self.logger = logging.getLogger(__name__)
        
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.exchange = exchange
        self.queue = queue
        self.routing_key = routing_key
        
        self.connection = None
        self.channel = None
    
    def connect(self):
        """
        Establish connection to RabbitMQ
        """
        try:
            self.logger.info(f"Connecting to RabbitMQ at {self.host}:{self.port}")
            
            # Create credentials
            credentials = pika.PlainCredentials(self.username, self.password)
            
            # Connection parameters
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            # Create connection
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declare exchange (idempotent)
            self.channel.exchange_declare(
                exchange=self.exchange,
                exchange_type='direct',
                durable=True
            )
            
            # Declare queue (idempotent)
            self.channel.queue_declare(
                queue=self.queue,
                durable=True
            )
            
            # Bind queue to exchange
            self.channel.queue_bind(
                exchange=self.exchange,
                queue=self.queue,
                routing_key=self.routing_key
            )
            
            self.logger.info("✓ Connected to RabbitMQ successfully")
            self.logger.info(f"  Exchange: {self.exchange}")
            self.logger.info(f"  Queue: {self.queue}")
            self.logger.info(f"  Routing Key: {self.routing_key}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            raise
    
    def get_message(self, auto_ack: bool = False) -> Optional[Dict[str, Any]]:
        """
        Get one message from queue (non-blocking)
        
        Args:
            auto_ack: Auto acknowledge message
            
        Returns:
            Dict containing:
                - id: Message ID (Guid)
                - path: S3 path
                - delivery_tag: For manual ack
                - message_raw: Raw message body
        """
        if not self.channel:
            self.connect()
        
        try:
            # Get one message from queue (non-blocking)
            method_frame, header_frame, body = self.channel.basic_get(
                queue=self.queue,
                auto_ack=auto_ack
            )
            
            if method_frame is None:
                self.logger.info("No messages in queue")
                return None
            
            self.logger.info(f"Received message from queue: {self.queue}")
            self.logger.info(f"Delivery tag: {method_frame.delivery_tag}")
            
            # Parse message body (JSON)
            try:
                message_data = json.loads(body.decode('utf-8'))
                self.logger.info(f"Message parsed: {message_data}")
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse message JSON: {str(e)}")
                self.logger.error(f"Raw body: {body}")
                
                # Reject malformed message
                if not auto_ack:
                    self.channel.basic_nack(
                        delivery_tag=method_frame.delivery_tag,
                        requeue=False
                    )
                return None
            
            # Extract XmlBigSizeMessageQueueDto fields
            message_id = message_data.get('Id') or message_data.get('id')
            s3_path = message_data.get('Path') or message_data.get('path')
            
            if not message_id or not s3_path:
                self.logger.error(f"Invalid message format - missing Id or Path")
                self.logger.error(f"Message data: {message_data}")
                
                # Reject invalid message
                if not auto_ack:
                    self.channel.basic_nack(
                        delivery_tag=method_frame.delivery_tag,
                        requeue=False
                    )
                return None
            
            self.logger.info(f"✓ Message received successfully")
            self.logger.info(f"  ID: {message_id}")
            self.logger.info(f"  Path: {s3_path}")
            
            return {
                'id': message_id,
                'path': s3_path,
                'delivery_tag': method_frame.delivery_tag,
                'message_raw': message_data,
                'received_at': datetime.utcnow().isoformat(),
                'exchange': method_frame.exchange,
                'routing_key': method_frame.routing_key,
                'redelivered': method_frame.redelivered
            }
            
        except Exception as e:
            self.logger.error(f"Error getting message from queue: {str(e)}")
            raise
    
    def acknowledge_message(self, delivery_tag: int):
        """
        Manually acknowledge message
        
        Args:
            delivery_tag: Delivery tag from get_message
        """
        try:
            self.channel.basic_ack(delivery_tag=delivery_tag)
            self.logger.info(f"Message acknowledged: delivery_tag={delivery_tag}")
        except Exception as e:
            self.logger.error(f"Failed to acknowledge message: {str(e)}")
            raise
    
    def reject_message(self, delivery_tag: int, requeue: bool = False):
        """
        Reject message
        
        Args:
            delivery_tag: Delivery tag from get_message
            requeue: Whether to requeue the message
        """
        try:
            self.channel.basic_nack(
                delivery_tag=delivery_tag,
                requeue=requeue
            )
            self.logger.info(f"Message rejected: delivery_tag={delivery_tag}, requeue={requeue}")
        except Exception as e:
            self.logger.error(f"Failed to reject message: {str(e)}")
            raise
    
    def get_queue_message_count(self) -> int:
        """
        Get number of messages in queue
        
        Returns:
            Number of messages
        """
        if not self.channel:
            self.connect()
        
        try:
            queue_state = self.channel.queue_declare(
                queue=self.queue,
                durable=True,
                passive=True
            )
            message_count = queue_state.method.message_count
            self.logger.info(f"Queue '{self.queue}' has {message_count} messages")
            return message_count
        except Exception as e:
            self.logger.error(f"Failed to get queue message count: {str(e)}")
            raise
    
    def close(self):
        """
        Close RabbitMQ connection
        """
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                self.logger.info("RabbitMQ connection closed")
        except Exception as e:
            self.logger.error(f"Error closing connection: {str(e)}")
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def create_rabbitmq_consumer(config: Dict[str, Any]) -> RabbitMQConsumer:
    """
    Factory function to create RabbitMQ consumer from config
    
    Args:
        config: Dict with keys: Exchange, Queue, RoutingKey, username, password, host, port
        
    Returns:
        RabbitMQConsumer instance
    """
    return RabbitMQConsumer(
        host=config.get('host', 'localhost'),
        port=config.get('port', 5672),
        username=config.get('username', 'guest'),
        password=config.get('password', 'guest'),
        exchange=config.get('Exchange', 'xml.exchange'),
        queue=config.get('Queue', 'xml.queue'),
        routing_key=config.get('RoutingKey', 'xml.key')
    )
