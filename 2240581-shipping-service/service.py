import pika, json, os, time, uuid
RABBIT = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq:5672/%2F")
params = pika.URLParameters(RABBIT)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='orders', exchange_type='topic', durable=True)
ch.queue_declare(queue='shipping-queue', durable=True)
ch.queue_bind(exchange='orders', queue='shipping-queue', routing_key='inventory.reserved')

processed = set()

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
    print("Published shipping.scheduled", out)

def callback(ch_, method, props, body):
    evt = json.loads(body)
    eid = evt["event_id"]
    if eid in processed:
        ch_.basic_ack(method.delivery_tag); return
    processed.add(eid)
    print("Shipping received", evt)
    time.sleep(0.5)
    publish_shipping(evt)
    ch_.basic_ack(method.delivery_tag)

ch.basic_consume('shipping-queue', callback)
print("Shipping Service listening...")
ch.start_consuming()
