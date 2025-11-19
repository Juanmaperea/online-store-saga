import pika, json, os, time, uuid, random
RABBIT = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq:5672/%2F")
params = pika.URLParameters(RABBIT)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='orders', exchange_type='topic', durable=True)
ch.queue_declare(queue='payment-queue', durable=True)
ch.queue_bind(exchange='orders', queue='payment-queue', routing_key='order.validated')

processed = set()

def publish_payment(evt, success=True):
    out = {
        "event_id": evt["event_id"],
        "type":"payment.completed" if success else "payment.failed",
        "order_id": evt["order_id"],
        "payment_id": str(uuid.uuid4()),
        "amount": evt.get("total"),
        "status": "COMPLETED" if success else "FAILED",
        "timestamp": int(time.time())
    }
    rk = 'payment.completed' if success else 'payment.failed'
    ch.basic_publish(exchange='orders', routing_key=rk, body=json.dumps(out))
    print("Published",rk,out)

def callback(ch_, method, props, body):
    evt = json.loads(body)
    eid = evt["event_id"]
    if eid in processed:
        ch_.basic_ack(method.delivery_tag); return
    processed.add(eid)
    print("Payment received", evt)
    # simulate payment success with 90% probability
    success = random.random() < 0.9
    time.sleep(1)
    publish_payment(evt, success)
    ch_.basic_ack(method.delivery_tag)

ch.basic_consume('payment-queue', callback)
print("Payment Service listening...")
ch.start_consuming()
