import pika, json, os, time
RABBIT = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq:5672/%2F")
params = pika.URLParameters(RABBIT)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='orders', exchange_type='topic', durable=True)
ch.queue_declare(queue='order-validator-queue', durable=True)
ch.queue_bind(exchange='orders', queue='order-validator-queue', routing_key='order.created')

processed = set()

def publish_validated(evt, valid=True):
    out = {
        "event_id": evt["event_id"],
        "type": "order.validated" if valid else "order.rejected",
        "order_id": evt["order_id"],
        "valid": valid,
        "timestamp": int(time.time())
    }
    rk = 'order.validated' if valid else 'order.rejected'
    ch.basic_publish(exchange='orders', routing_key=rk, body=json.dumps(out))
    print("Published", rk, out)

def callback(ch_, method, props, body):
    evt = json.loads(body)
    eid = evt["event_id"]
    if eid in processed:
        ch_.basic_ack(method.delivery_tag); return
    processed.add(eid)
    print("Validator received", evt)
    valid = (evt.get("total",0) > 0 and len(evt.get("items",[]))>0)
    publish_validated(evt, valid)
    ch_.basic_ack(method.delivery_tag)

ch.basic_consume('order-validator-queue', callback)
print("Order Validator listening...")
ch.start_consuming()
