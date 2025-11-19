import pika, json, os, time, uuid, random
import logging 
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [2240581-SHIPPING-SERVICE] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
) 
RABBIT = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq:5672/%2F")
params = pika.URLParameters(RABBIT)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='orders', exchange_type='topic', durable=True)
ch.queue_declare(queue='shipping-queue', durable=True)
ch.queue_bind(exchange='orders', queue='shipping-queue', routing_key='inventory.reserved')

FAIL_PROB = float(os.getenv("FAIL_PROB", "0.40"))  

processed = set()

def publish_failure(evt, reason="Shipping failure"):
    out = {
        "event_id": evt["event_id"],
        "type": "order.failed",
        "order_id": evt["order_id"],
        "failed_at": "shipping",
        "reason": reason,
        "timestamp": int(time.time())
    }
    ch.basic_publish(exchange='orders', routing_key='order.failed', body=json.dumps(out))
    logging.error("EVENT - SHIPPING FAILED for order %s | reason=%s", evt["order_id"], reason)
    return out

def publish_shipping(evt):
    out = {
        "event_id": evt["event_id"],
        "type":"shipping.scheduled",
        "order_id": evt["order_id"],
        "shipment_id": str(uuid.uuid4()),
        "eta_days": 3,
        "timestamp": int(time.time())
    }
    ch.basic_publish(exchange='orders', routing_key='shipping.scheduled', body=json.dumps(out))
    logging.info("EVENT - Published shipping.scheduled: %s", out)

def callback(ch_, method, props, body):
    evt = json.loads(body)
    eid = evt["event_id"]
    if eid in processed:
        ch_.basic_ack(method.delivery_tag); return
    processed.add(eid)
    logging.info("EVENT - Shipping received: %s", evt)
    if random.random() < FAIL_PROB:
        publish_failure(evt, reason="Random shipping failure")
        ch_.basic_ack(method.delivery_tag)
        return
    time.sleep(0.5)
    publish_shipping(evt)
    ch_.basic_ack(method.delivery_tag)

ch.basic_consume('shipping-queue', callback)
logging.info("Shipping Service listening...")
ch.start_consuming()
