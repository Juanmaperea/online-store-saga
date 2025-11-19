import pika, json, os
RABBIT = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq:5672/%2F")
params = pika.URLParameters(RABBIT)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='orders', exchange_type='topic', durable=True)
ch.queue_declare(queue='order-history-queue', durable=True)
ch.queue_bind(exchange='orders', queue='order-history-queue', routing_key='order.*')
ch.queue_bind(exchange='orders', queue='order-history-queue', routing_key='payment.*')
ch.queue_bind(exchange='orders', queue='order-history-queue', routing_key='inventory.*')
ch.queue_bind(exchange='orders', queue='order-history-queue', routing_key='shipping.*')

processed = set()
store = {}

def callback(ch_, method, props, body):
    evt = json.loads(body)
    eid = evt.get("event_id")
    if eid in processed:
        ch_.basic_ack(method.delivery_tag); return
    processed.add(eid)
    order_id = evt.get("order_id")
    store.setdefault(order_id, []).append(evt)
    print("History stored event for", order_id, "type:", evt.get("type"))
    ch_.basic_ack(method.delivery_tag)

ch.basic_consume('order-history-queue', callback)
print("Order History listening...")
ch.start_consuming()
