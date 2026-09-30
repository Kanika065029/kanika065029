import json
import mysql.connector
from kafka import KafkaConsumer

# ==============================
# CONFIGURATION
# ==============================

KAFKA_TOPIC = "order-events-topic"
KAFKA_SERVER = "127.0.0.1:9092"

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DATABASE = "stockdb"

# ==============================
# CONNECT TO MYSQL
# ==============================

try:
    db = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

    cursor = db.cursor()
    print("Connected to MySQL successfully.")

except Exception as e:
    print("MySQL connection error:", e)
    exit()

# ==============================
# CONNECT TO KAFKA
# ==============================

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="assignment3-consumer",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    print(f"Connected to Kafka.")
    print(f"Listening to topic: {KAFKA_TOPIC}")
    print("Waiting for messages...\n")

except Exception as e:
    print("Kafka connection error:", e)
    exit()

# ==============================
# GET MYSQL TABLE COLUMNS
# ==============================

cursor.execute("DESCRIBE order_events")
table_columns = {row[0] for row in cursor.fetchall()}

print("MySQL table columns detected:")
print(table_columns)
print()

# ==============================
# READ KAFKA MESSAGES
# ==============================

count = 0

for message in consumer:

    data = message.value

    # Keep only fields that actually exist in MySQL
    filtered_data = {
        key: value
        for key, value in data.items()
        if key in table_columns
    }

    if not filtered_data:
        print("No matching fields found.")
        continue

    columns = list(filtered_data.keys())
    values = list(filtered_data.values())

    # Convert timestamp into MySQL-compatible format
    if "status_timestamp" in filtered_data:
        timestamp_value = filtered_data["status_timestamp"]

        if isinstance(timestamp_value, str):
            filtered_data["status_timestamp"] = timestamp_value.replace("T", " ")

        values = list(filtered_data.values())

    column_names = ", ".join(f"`{col}`" for col in columns)
    placeholders = ", ".join(["%s"] * len(columns))

    query = f"""
        INSERT INTO order_events
        ({column_names})
        VALUES ({placeholders})
    """

    try:
        cursor.execute(query, values)
        db.commit()

        count += 1

        print(
            f"Inserted message #{count} | "
            f"event_id={data.get('event_id')} | "
            f"order_id={data.get('order_id')} | "
            f"status={data.get('order_status')}"
        )

    except Exception as e:
        print("Database insert error:", e)