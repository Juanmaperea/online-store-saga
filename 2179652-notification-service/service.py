import pika, json, os, time 
import logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [2179652-NOTIFICATION-SERVICE] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
RABBIT = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq:5672/%2F")
params = pika.URLParameters(RABBIT)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='orders', exchange_type='topic', durable=True)
ch.queue_declare(queue='notification-queue', durable=True)
ch.queue_bind(exchange='orders', queue='notification-queue', routing_key='shipping.scheduled')
ch.queue_bind(exchange='orders', queue='notification-queue', routing_key='payment.failed')
ch.queue_bind(exchange='orders', queue='notification-queue', routing_key='inventory.failed') 
ch.queue_bind(exchange='orders', queue='notification-queue', routing_key='order.failed')

processed = set()

def publish_notification(evt, message):
    out = {
        "event_id": evt.get("event_id"),
        "type":"notification.sent",
        "order_id": evt.get("order_id"),
        "message": message,
        "timestamp": int(time.time())
    }
    ch.basic_publish(exchange='orders', routing_key='notification.sent', body=json.dumps(out))
    logging.info("EVENT - Notification published: %s", out)

def callback(ch_, method, props, body):
    evt = json.loads(body)
    eid = evt.get("event_id")
    if eid in processed:
        ch_.basic_ack(method.delivery_tag); return
    processed.add(eid)
    logging.info("EVENT - Notification received event: %s", evt.get("type"))
    if evt.get("type") == "shipping.scheduled":
        publish_notification(evt, "Your order has been shipped!")
    else:
        publish_notification(evt, "There was a problem with your order. Contact support.")
    ch_.basic_ack(method.delivery_tag)

ch.basic_consume('notification-queue', callback)
logging.info("Notification Service listening...")
ch.start_consuming()
