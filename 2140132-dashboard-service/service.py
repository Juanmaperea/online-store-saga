import pika, json, os, threading
from flask import Flask, jsonify

RABBIT = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq:5672/%2F")
params = pika.URLParameters(RABBIT)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='orders', exchange_type='topic', durable=True)
ch.queue_declare(queue='dashboard-queue', durable=True)
ch.queue_bind(exchange='orders', queue='dashboard-queue', routing_key='#')  # all events

store = []

def callback(ch_, method, props, body):
    evt = json.loads(body)
    store.append(evt)
    print("Dashboard received:", evt.get("type"))
    ch_.basic_ack(method.delivery_tag)

def start_consumer():
    ch.basic_consume('dashboard-queue', callback)
    ch.start_consuming()

app = Flask(__name__)

@app.route("/events")
def events():
    return jsonify(store[-50:])

if __name__ == "__main__":
    t = threading.Thread(target=start_consumer, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=80)