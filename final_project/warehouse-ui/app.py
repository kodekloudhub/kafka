from flask import Flask, render_template, jsonify
from confluent_kafka import Consumer
import json
import logging
import uuid

logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

KAFKA_TOPIC = 'cartevent'

def get_kafka_messages():
    """Get all available messages from Kafka"""
    messages = []
    
    # Create consumer with unique group ID
    consumer_config = {
        'bootstrap.servers': '3.68.92.91:9092',
        'group.id': f'warehouse_reader_{uuid.uuid4()}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    consumer = Consumer(consumer_config)
    logger.info("Created new Kafka consumer")
    
    try:
        consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")
        
        # Poll for messages with a timeout
        for _ in range(10):  # Try 10 times
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                logger.error(f'Error: {msg.error()}')
                continue
                
            try:
                value = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Reading message for customer: {value.get('customer_name')}")
                messages.append(value)
            except json.JSONDecodeError as e:
                logger.error(f'Failed to parse message: {e}')
                
    except Exception as e:
        logger.error(f"Error reading messages: {e}")
    finally:
        try:
            consumer.close()
            logger.info("Consumer closed successfully")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")
        
    logger.info(f"Retrieved {len(messages)} messages")
    return messages

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_orders')
def get_orders():
    try:
        messages = get_kafka_messages()
        logger.info(f"Sending {len(messages)} orders to UI")
        if messages:
            logger.info(f"Sample message: {messages[0]}")
        return jsonify(messages)
    except Exception as e:
        logger.error(f"Error in get_orders: {e}")
        return jsonify([])

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5003)