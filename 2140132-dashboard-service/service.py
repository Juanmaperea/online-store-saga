import pika, json, os, threading
from flask import Flask, jsonify 
import logging 
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [2140132-DASHBOARD-SERVICE] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

RABBIT = os.getenv("RABBIT_URL","amqp://guest:guest@rabbitmq:5672/%2F")
params = pika.URLParameters(RABBIT)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.exchange_declare(exchange='orders', exchange_type='topic', durable=True)
ch.queue_declare(queue='dashboard-queue', durable=True)
ch.queue_bind(exchange='orders', queue='dashboard-queue', routing_key='#')  # all events

store = [] 

order_status = {}      # {"order_id": "success" / "failed" / "processing"}
order_finalized = set()  # para no imprimir el final más de una vez


def print_final_status(order_id):
    """Imprime una sola vez el resultado final de la orden."""
    if order_id in order_finalized:
        return
    order_finalized.add(order_id)

    status = order_status.get(order_id, "unknown")

    logging.info("")
    logging.info("==========================================")
    if status == "success":
        logging.info("🎉🎉  ORDER %s COMPLETED SUCCESSFULLY  🎉🎉", str(order_id).upper())
    elif status == "failed":
        logging.info("❌❌  ORDER %s FAILED DURING PROCESSING ❌❌", str(order_id).upper())
    else:
        logging.info("⚠⚠  ORDER %s STATUS UNKNOWN ⚠⚠", str(order_id).upper())
    logging.info("==========================================")
    logging.info("")

def callback(ch_, method, props, body):
    evt = json.loads(body)
    store.append(evt) 
    order_id = evt.get("order_id", "UNKNOWN")
    event_type = evt.get("type", "unknown")
    logging.info("")
    logging.info("==========================================")
    logging.info("🔥 PROCESSING ORDER %s | EVENT: %s 🔥", str(order_id).upper(), event_type.upper())
    logging.info("==========================================")
    print("Dashboard received:", evt.get("type")) 
    logging.info("EVENT - Dashboard received: %s", evt.get("type")) 
    if event_type == "order.failed":
        order_status[order_id] = "failed"
        print_final_status(order_id)

    # Si llega shipping.scheduled → la orden terminó bien
    elif event_type == "shipping.scheduled":
        # Solo marcar éxito si no había fallado antes
        if order_status.get(order_id) != "failed":
            order_status[order_id] = "success"
        print_final_status(order_id)
    else:
        if order_id not in order_status:
            order_status[order_id] = "processing"
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