import pika, json, os, time, uuid
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
    print("Published order.created", evt)

if __name__ == "__main__":
    # demo: publish one order
    create_order("ord-123", "user-7", [{"sku":"A1","qty":2},{"sku":"B2","qty":1}], 59.90)
    conn.close()
