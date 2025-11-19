#!/bin/bash
echo "Build images..."
docker build -t online-store/dashboard-service:dev ./2140132-dashboard-service
docker build -t online-store/checkout-service:dev ./1926462-checkout-service
docker build -t online-store/order-validator:dev ./2159832-order-validator
docker build -t online-store/payment-service:dev ./2182527-payment-service
docker build -t online-store/inventory-service:dev ./1832375-inventory-service
docker build -t online-store/shipping-service:dev ./2240581-shipping-service
docker build -t online-store/order-history:dev ./2259395-order-history
docker build -t online-store/notification-service:dev ./2179652-notification-service

echo "Apply RabbitMQ..."
kubectl apply -f k8s/rabbitmq-deployment.yaml

echo "Apply services..."
for f in k8s/*-deployment.yaml; do kubectl apply -f "$f"; done

echo "Port-forward rabbitmq to 15672 (open another terminal)"
echo "kubectl port-forward svc/rabbitmq 15672:15672"
echo "Then run: kubectl exec deployment/checkout-service -- python service.py"

