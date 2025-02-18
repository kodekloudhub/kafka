from flask import Flask, render_template, request, redirect, url_for
from confluent_kafka import Producer
import json
import logging
import socket

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Kafka producer configuration
conf = {
    'bootstrap.servers': '3.68.92.91:9092',
    'client.id': socket.gethostname()
}

# Initialize producer instance
producer = Producer(conf)

# Delivery callback
def delivery_callback(err, msg):
    if err:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        logger.info(f'Message timestamp: {msg.timestamp()}')

# Sample product data
products = [
    {"id": 1, "name": "Toy 1", "price": 10.99, "image": "toy1.jpg"},
    {"id": 2, "name": "Toy 2", "price": 15.99, "image": "toy2.jpg"},
    {"id": 3, "name": "Toy 3", "price": 20.99, "image": "toy3.jpg"},
    {"id": 4, "name": "Toy 4", "price": 25.99, "image": "toy4.jpg"},
    {"id": 5, "name": "Toy 5", "price": 30.99, "image": "toy5.jpg"},
    {"id": 6, "name": "Toy 6", "price": 35.99, "image": "toy6.jpg"},
]

cart = []

@app.route('/')
def index():
    return render_template('index.html', products=products)

@app.route('/add_to_cart/<int:product_id>')
def add_to_cart(product_id):
    product = next((p for p in products if p['id'] == product_id), None)
    if product:
        cart.append(product)
    return redirect(url_for('index'))

@app.route('/cart')
def view_cart():
    return render_template('cart.html', cart=cart)

@app.route('/place_order', methods=['POST'])
def place_order():
    try:
        name = request.form['name']
        address = request.form['address']
        
        # Prepare the event payload
        order_event = {
            'customer_name': name,
            'delivery_address': address,
            'products': cart,
            'total_amount': sum(item['price'] for item in cart)
        }
        
        # Convert the event to JSON string
        event_string = json.dumps(order_event)
        
        # Log the event being sent
        logger.info(f"Sending order event to Kafka: {event_string}")
        
        # Produce the message to Kafka
        producer.produce(
            topic='cartevent',
            value=event_string.encode('utf-8'),
            callback=delivery_callback
        )
        
        # Wait for message delivery
        producer.flush()
        
        # Clear the cart after successful order placement
        cart.clear()
        
        return render_template('order.html', name=name)
    
    except Exception as e:
        logger.error(f"Failed to process order: {str(e)}")
        return render_template('error.html', error=str(e))

if __name__ == '__main__':
    app.run(debug=True)