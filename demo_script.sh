#!/bin/bash
echo "Build images..."
docker build -t online-store/checkout-service:dev ./1926462-checkout-service 
docker build -t online-store/shipping-service:dev ./2240581-shipping-service

echo "Apply RabbitMQ..."
kubectl apply -f k8s/rabbitmq-deployment.yaml

echo "Apply services..."
for f in k8s/*-deployment.yaml; do kubectl apply -f "$f"; done

echo "Port-forward rabbitmq to 15672 (open another terminal)"
echo "kubectl port-forward svc/rabbitmq 15672:15672"
echo "Then run: kubectl exec deployment/checkout-service -- python service.py"
