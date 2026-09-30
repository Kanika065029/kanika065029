# Food Delivery Kafka Streaming – Assignment 3

## Project Overview
This project implements a real-time food delivery data pipeline using Apache Kafka, Python, MySQL, and Grafana.

## Data Flow
Assignment 2 JSONL data → Python Producer → Kafka Topics → Python Consumer → MySQL → Grafana Dashboard

## Kafka Topics
- order-events-topic
- delivery-tracking-topic

## Technologies Used
- Python
- Apache Kafka
- MySQL
- Grafana
- Docker

## Dashboard
The Grafana dashboard provides real-time business insights through:

1. Orders by City
2. Total Order Value by Payment Mode
3. Average Order Value by Cuisine
4. Average Restaurant Preparation Time by City

## Consumer
`consumer.py` consumes order-event messages from Kafka and stores the relevant data in the MySQL `order_events` table for dashboard analysis.

## Business Insights
- Order activity can be compared across cities to identify locations with higher demand.
- Payment-mode distribution shows how customers are completing their orders.
- Average order value by cuisine helps identify cuisines generating higher-value orders.
- Restaurant preparation time by city provides visibility into operational efficiency.
