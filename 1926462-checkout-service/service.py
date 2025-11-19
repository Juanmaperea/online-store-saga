import pika, json, os, time, uuid, random
import logging 
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [1926462-CHECKOUT-SERVICE] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)  

def generate_order_id():
    return f"ORD-{random.randint(1000, 9999)}"

PRODUCTS = [
    {"sku": "A1", "name": "Keyboard",       "price": 25.0},
    {"sku": "B2", "name": "Mouse",          "price": 15.0},
    {"sku": "C3", "name": "USB Cable",      "price": 5.0},
    {"sku": "D4", "name": "Headphones",     "price": 40.0},
    {"sku": "E5", "name": "Webcam",         "price": 30.0},
    {"sku": "F6", "name": "Monitor Stand",  "price": 20.0}
]

def generate_random_items():
    items = random.sample(PRODUCTS, random.randint(1, 3))
    for item in items:
        item["qty"] = random.randint(1, 3)
    return items

def calculate_total(items):
    return sum(item["price"] * item["qty"] for item in items) 

def random_user():
    return f"user-{random.randint(1, 20)}"

RABBIT = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq:5672/%2F")
params = pika.URLParameters(RABBIT)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='orders', exchange_type='topic', durable=True)

def create_order(order_id, user_id, items, total):
    evt = {
        "event_id": str(uuid.uuid4()),
        "type": "order.created",
        "order_id": order_id,
        "user_id": user_id,
        "items": items,
        "total": total,
        "timestamp": int(time.time())
    }
    ch.basic_publish(exchange='orders', routing_key='order.created', body=json.dumps(evt))
    logging.info("EVENT - Published order.created: %s", evt)

if __name__ == "__main__":
    # demo: publish one order
    items = generate_random_items()
    total = calculate_total(items)
    user = random_user()
    order_id = generate_order_id()

    create_order(order_id, user, items, total)
    conn.close()

