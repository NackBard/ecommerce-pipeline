from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import psycopg2
from confluent_kafka import Consumer
import logging
logger = logging.getLogger(__name__)

DB_CONN = "postgresql://user:password@postgres/analytics"

def consume_from_kafka(**context):
    conn = None
    consumer = None
    try:
        consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'airflow-processor',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        consumer.subscribe(['ecommerce-events'])

        conn = psycopg2.connect(DB_CONN)
        cur = conn.cursor()

        batch = []
        deadline = datetime.now() + timedelta(seconds=300)

        processed = 0

        while datetime.now() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue

            event = json.loads(msg.value())
            batch.append((
                event['event_type'],
                event['user_id'],
                json.dumps(event['payload']),
                event['created_at']
            ))

            if len(batch) >= 1000:
                _insert_batch(cur, batch)
                processed += len(batch)
                batch.clear()

        if batch:
            _insert_batch(cur, batch)
            processed += len(batch)

        conn.commit()
        if processed > 0:
            consumer.commit()

        print(f"Processed {processed} events")
    except Exception as e:
        logger.error(f"Error processing Kafka messages: {e}", exc_info=True)
        if conn:
            conn.rollback() 
        raise 
    finally:
        if conn:
            conn.close()
        if consumer:
            consumer.close()


def _insert_batch(cur, batch):
    cur.executemany("""
        INSERT INTO raw.events (event_type, user_id, payload, created_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, batch)


def build_marts(**context):
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO mart.daily_revenue (date, orders_count, revenue, avg_order_value)
        SELECT
            created_at::date,
            COUNT(*),
            SUM((payload->>'amount')::numeric),
            AVG((payload->>'amount')::numeric)
        FROM raw.events
        WHERE event_type = 'order_paid'
        GROUP BY created_at::date
        ON CONFLICT (date) DO UPDATE
            SET orders_count    = EXCLUDED.orders_count,
                revenue         = EXCLUDED.revenue,
                avg_order_value = EXCLUDED.avg_order_value
    """)

    cur.execute("""
        INSERT INTO mart.conversion (date, views_count, orders_count, conversion_rate)
        SELECT
            date,
            views,
            orders,
            ROUND(orders * 100.0 / NULLIF(views, 0), 2)
        FROM (
            SELECT
                created_at::date as date,
                COUNT(*) FILTER (WHERE event_type = 'item_viewed')  as views,
                COUNT(*) FILTER (WHERE event_type = 'order_created') as orders
            FROM raw.events
            GROUP BY created_at::date
        ) sub
        ON CONFLICT (date) DO UPDATE
            SET views_count     = EXCLUDED.views_count,
                orders_count    = EXCLUDED.orders_count,
                conversion_rate = EXCLUDED.conversion_rate
    """)

    cur.execute("""
                INSERT INTO mart.cohort_ltv (cohort_month, order_month, users_count, revenue)
        SELECT
            cohort_month,
            order_month,
			COUNT(DISTINCT orders.user_id) AS users_count,
            SUM(revenue) AS revenue
        FROM (
            SELECT DISTINCT
                DATE_TRUNC('month', created_at)::DATE as cohort_month,
                user_id
            FROM raw.events
			WHERE event_type = 'user_registered'
        ) users
        INNER JOIN (
                SELECT
                    DATE_TRUNC('month', created_at)::DATE as order_month,
					user_id,
                    (payload->>'amount')::NUMERIC AS revenue
                    FROM raw.events
                    WHERE event_type = 'order_paid'
                ) orders ON users.user_id = orders.user_id
        WHERE order_month >= cohort_month
		GROUP BY cohort_month, order_month 
        ORDER BY cohort_month, order_month
        ON CONFLICT (cohort_month, order_month) DO UPDATE
            SET users_count     = EXCLUDED.users_count,
                revenue    = EXCLUDED.revenue
    """)

    conn.commit()


with DAG(
    dag_id='ecommerce_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:

    ingest = PythonOperator(
        task_id='consume_kafka',
        python_callable=consume_from_kafka
    )

    aggregate = PythonOperator(
        task_id='build_marts',
        python_callable=build_marts
    )

    ingest >> aggregate