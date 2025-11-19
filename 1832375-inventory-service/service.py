import pika, json, os, time, random 
import logging  
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [1832375-INVENTORY-SERVICE] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
RABBIT = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq:5672/%2F")
params = pika.URLParameters(RABBIT)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='orders', exchange_type='topic', durable=True)
ch.queue_declare(queue='inventory-queue', durable=True)
ch.queue_bind(exchange='orders', queue='inventory-queue', routing_key='payment.completed')

processed = set()

def publish_inventory(evt, ok=True):
    out = {
        "event_id": evt["event_id"],
        "type": "inventory.reserved" if ok else "inventory.failed",
        "order_id": evt["order_id"],
        "timestamp": int(time.time())
    }
    rk = 'inventory.reserved' if ok else 'inventory.failed'
    ch.basic_publish(exchange='orders', routing_key=rk, body=json.dumps(out))
    logging.info("EVENT - Published: %s | %s", rk, out)

def callback(ch_, method, props, body):
    evt = json.loads(body)
    eid = evt["event_id"]
    if eid in processed:
        ch_.basic_ack(method.delivery_tag); return
    processed.add(eid)
    logging.info("EVENT - Inventory received: %s", evt)
    ok = random.random() < 0.95
    time.sleep(0.5)
    publish_inventory(evt, ok)
    ch_.basic_ack(method.delivery_tag)

ch.basic_consume('inventory-queue', callback)
logging.info("Inventory Service listening...")
ch.start_consuming()
