from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pymongo
import psycopg2
import json
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def get_mongo_connection():
    client = pymongo.MongoClient(
        host='mongodb',
        port=27017,
        username='admin',
        password='admin123',
        authSource='admin',
        authMechanism='SCRAM-SHA-256',
        serverSelectionTimeoutMS=5000
    )
    client.admin.command('ping')
    return client

def get_postgres_connection():
    conn = psycopg2.connect(
        host='postgres-analytics',
        port=5432,
        database='analytics_warehouse',
        user='analytics_user',
        password='analytics_password'
    )
    return conn

def replicate_user_sessions(**context):
    logger.info("Начинаем репликацию UserSessions...")
    
    mongo_client = get_mongo_connection()
    db = mongo_client['analytics_source']
    collection = db['UserSessions']
    
    sessions = list(collection.find({}, {'_id': 0}))
    logger.info(f"Найдено {len(sessions)} сессий в MongoDB")
    
    if not sessions:
        mongo_client.close()
        return
    
    transformed_data = []
    for session in sessions:
        try:
            transformed_data.append((
                session['session_id'],
                session['user_id'],
                datetime.fromisoformat(session['start_time'].replace('Z', '+00:00')),
                datetime.fromisoformat(session['end_time'].replace('Z', '+00:00')) if session.get('end_time') else None,
                session.get('pages_visited', []),
                session.get('device', 'unknown'),
                session.get('actions', [])
            ))
        except Exception as e:
            logger.error(f"Ошибка при обработке сессии {session.get('session_id')}: {e}")
    
    pg_conn = get_postgres_connection()
    cursor = pg_conn.cursor()
    
    insert_sql = """
        INSERT INTO raw_data.user_sessions 
        (session_id, user_id, start_time, end_time, pages_visited, device, actions)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (session_id) DO UPDATE SET 
            user_id = EXCLUDED.user_id,
            end_time = EXCLUDED.end_time,
            pages_visited = EXCLUDED.pages_visited,
            device = EXCLUDED.device,
            actions = EXCLUDED.actions,
            loaded_at = CURRENT_TIMESTAMP;
    """
    
    cursor.executemany(insert_sql, transformed_data)
    pg_conn.commit()
    
    logger.info(f"Загружено {len(transformed_data)} сессий в PostgreSQL")
    
    cursor.close()
    pg_conn.close()
    mongo_client.close()

def replicate_event_logs(**context):
    logger.info("Начинаем репликацию EventLogs...")
    
    mongo_client = get_mongo_connection()
    db = mongo_client['analytics_source']
    collection = db['EventLogs']
    
    events = list(collection.find({}, {'_id': 0}))
    logger.info(f"Найдено {len(events)} событий в MongoDB")
    
    if not events:
        mongo_client.close()
        return
    
    transformed_data = []
    for event in events:
        try:
            transformed_data.append((
                event['event_id'],
                datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')),
                event.get('event_type', 'unknown'),
                json.dumps(event.get('details', {}))
            ))
        except Exception as e:
            logger.error(f"Ошибка при обработке события {event.get('event_id')}: {e}")
    
    pg_conn = get_postgres_connection()
    cursor = pg_conn.cursor()
    
    insert_sql = """
        INSERT INTO raw_data.event_logs 
        (event_id, timestamp, event_type, details)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (event_id) DO UPDATE SET 
            timestamp = EXCLUDED.timestamp,
            event_type = EXCLUDED.event_type,
            details = EXCLUDED.details,
            loaded_at = CURRENT_TIMESTAMP;
    """
    
    cursor.executemany(insert_sql, transformed_data)
    pg_conn.commit()
    
    logger.info(f"Загружено {len(transformed_data)} событий в PostgreSQL")
    
    cursor.close()
    pg_conn.close()
    mongo_client.close()

def replicate_support_tickets(**context):
    logger.info("Начинаем репликацию SupportTickets...")
    
    mongo_client = get_mongo_connection()
    db = mongo_client['analytics_source']
    collection = db['SupportTickets']
    
    tickets = list(collection.find({}, {'_id': 0}))
    logger.info(f"Найдено {len(tickets)} тикетов в MongoDB")
    
    if not tickets:
        mongo_client.close()
        return
    
    transformed_data = []
    for ticket in tickets:
        try:
            transformed_data.append((
                ticket['ticket_id'],
                ticket['user_id'],
                ticket.get('status', 'unknown'),
                ticket.get('issue_type', 'unknown'),
                json.dumps(ticket.get('messages', [])),
                datetime.fromisoformat(ticket['created_at'].replace('Z', '+00:00')),
                datetime.fromisoformat(ticket['updated_at'].replace('Z', '+00:00')) if ticket.get('updated_at') else None
            ))
        except Exception as e:
            logger.error(f"Ошибка при обработке тикета {ticket.get('ticket_id')}: {e}")
    
    pg_conn = get_postgres_connection()
    cursor = pg_conn.cursor()
    
    insert_sql = """
        INSERT INTO raw_data.support_tickets 
        (ticket_id, user_id, status, issue_type, messages, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticket_id) DO UPDATE SET 
            user_id = EXCLUDED.user_id,
            status = EXCLUDED.status,
            issue_type = EXCLUDED.issue_type,
            messages = EXCLUDED.messages,
            updated_at = EXCLUDED.updated_at,
            loaded_at = CURRENT_TIMESTAMP;
    """
    
    cursor.executemany(insert_sql, transformed_data)
    pg_conn.commit()
    
    logger.info(f"Загружено {len(transformed_data)} тикетов в PostgreSQL")
    
    cursor.close()
    pg_conn.close()
    mongo_client.close()

def replicate_user_recommendations(**context):
    logger.info("Начинаем репликацию UserRecommendations...")
    
    mongo_client = get_mongo_connection()
    db = mongo_client['analytics_source']
    collection = db['UserRecommendations']
    
    recommendations = list(collection.find({}, {'_id': 0}))
    logger.info(f"Найдено {len(recommendations)} рекомендаций в MongoDB")
    
    if not recommendations:
        mongo_client.close()
        return
    
    transformed_data = []
    for rec in recommendations:
        try:
            transformed_data.append((
                rec['user_id'],
                rec.get('recommended_products', []),
                datetime.fromisoformat(rec['last_updated'].replace('Z', '+00:00')) if rec.get('last_updated') else None
            ))
        except Exception as e:
            logger.error(f"Ошибка при обработке рекомендаций для пользователя {rec.get('user_id')}: {e}")
    
    pg_conn = get_postgres_connection()
    cursor = pg_conn.cursor()
    
    insert_sql = """
        INSERT INTO raw_data.user_recommendations 
        (user_id, recommended_products, last_updated)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET 
            recommended_products = EXCLUDED.recommended_products,
            last_updated = EXCLUDED.last_updated,
            loaded_at = CURRENT_TIMESTAMP;
    """
    
    cursor.executemany(insert_sql, transformed_data)
    pg_conn.commit()
    
    logger.info(f"Загружено {len(transformed_data)} рекомендаций в PostgreSQL")
    
    cursor.close()
    pg_conn.close()
    mongo_client.close()

def replicate_moderation_queue(**context):
    logger.info("Начинаем репликацию ModerationQueue...")
    
    mongo_client = get_mongo_connection()
    db = mongo_client['analytics_source']
    collection = db['ModerationQueue']
    
    reviews = list(collection.find({}, {'_id': 0}))
    logger.info(f"Найдено {len(reviews)} отзывов в MongoDB")
    
    if not reviews:
        mongo_client.close()
        return
    
    transformed_data = []
    for review in reviews:
        try:
            transformed_data.append((
                review['review_id'],
                review.get('user_id'),
                review.get('product_id'),
                review.get('review_text', ''),
                review.get('rating', 0),
                review.get('moderation_status', 'pending'),
                review.get('flags', []),
                datetime.fromisoformat(review['submitted_at'].replace('Z', '+00:00')) if review.get('submitted_at') else None
            ))
        except Exception as e:
            logger.error(f"Ошибка при обработке отзыва {review.get('review_id')}: {e}")
    
    pg_conn = get_postgres_connection()
    cursor = pg_conn.cursor()
    
    insert_sql = """
        INSERT INTO raw_data.moderation_queue 
        (review_id, user_id, product_id, review_text, rating, moderation_status, flags, submitted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (review_id) DO UPDATE SET 
            user_id = EXCLUDED.user_id,
            product_id = EXCLUDED.product_id,
            review_text = EXCLUDED.review_text,
            rating = EXCLUDED.rating,
            moderation_status = EXCLUDED.moderation_status,
            flags = EXCLUDED.flags,
            submitted_at = EXCLUDED.submitted_at,
            loaded_at = CURRENT_TIMESTAMP;
    """
    
    cursor.executemany(insert_sql, transformed_data)
    pg_conn.commit()
    
    logger.info(f"Загружено {len(transformed_data)} отзывов в PostgreSQL")
    
    cursor.close()
    pg_conn.close()
    mongo_client.close()

def build_user_activity_mart(**context):
    logger.info("Начинаем построение витрины user_activity...")
    
    pg_conn = get_postgres_connection()
    cursor = pg_conn.cursor()
    
    cursor.execute("TRUNCATE mart.user_activity;")
    
    query = """
        INSERT INTO mart.user_activity 
        (date, user_id, total_sessions, avg_session_duration_minutes, 
         total_pages_visited, unique_pages_visited, total_actions, 
         most_frequent_action, devices_used, last_activity)
        
        WITH session_stats AS (
            SELECT 
                DATE(s.start_time) as date,
                s.user_id,
                COUNT(DISTINCT s.session_id) as total_sessions,
                COALESCE(AVG(EXTRACT(EPOCH FROM (s.end_time - s.start_time)) / 60), 0) as avg_session_duration,
                COALESCE(SUM(array_length(s.pages_visited, 1)), 0) as total_pages,
                COUNT(DISTINCT unnest_pages.page) as unique_pages,
                COALESCE(SUM(array_length(s.actions, 1)), 0) as total_actions,
                MODE() WITHIN GROUP (ORDER BY unnest_actions.action) as most_freq_action,
                array_agg(DISTINCT s.device) as devices,
                MAX(s.end_time) as last_activity
            FROM raw_data.user_sessions s
            LEFT JOIN LATERAL unnest(s.pages_visited) AS unnest_pages(page) ON TRUE
            LEFT JOIN LATERAL unnest(s.actions) AS unnest_actions(action) ON TRUE
            GROUP BY DATE(s.start_time), s.user_id
        )
        SELECT 
            date,
            user_id,
            total_sessions,
            ROUND(avg_session_duration::numeric, 2) as avg_session_duration_minutes,
            total_pages as total_pages_visited,
            unique_pages as unique_pages_visited,
            total_actions,
            most_freq_action as most_frequent_action,
            devices as devices_used,
            last_activity
        FROM session_stats
        ORDER BY date, user_id;
    """
    
    cursor.execute(query)
    pg_conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM mart.user_activity")
    count = cursor.fetchone()[0]
    logger.info(f"Витрина user_activity заполнена: {count} записей")
    
    cursor.close()
    pg_conn.close()

def build_support_efficiency_mart(**context):
    logger.info("Начинаем построение витрины support_efficiency...")
    
    pg_conn = get_postgres_connection()
    cursor = pg_conn.cursor()
    
    cursor.execute("TRUNCATE mart.support_efficiency;")
    
    query = """
        INSERT INTO mart.support_efficiency 
        (date, issue_type, total_tickets, open_tickets, closed_tickets, 
         avg_resolution_time_hours, max_resolution_time_hours, 
         min_resolution_time_hours, total_messages, unique_users)
        
        WITH ticket_stats AS (
            SELECT 
                DATE(created_at) as date,
                issue_type,
                COUNT(*) as total_tickets,
                COUNT(CASE WHEN status = 'open' THEN 1 END) as open_tickets,
                COUNT(CASE WHEN status = 'closed' THEN 1 END) as closed_tickets,
                AVG(CASE 
                    WHEN status = 'closed' AND updated_at > created_at
                    THEN EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600.0 
                END) as avg_resolution_hours,
                MAX(CASE 
                    WHEN status = 'closed' AND updated_at > created_at
                    THEN EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600.0 
                END) as max_resolution_hours,
                MIN(CASE 
                    WHEN status = 'closed' AND updated_at > created_at
                    THEN EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600.0 
                END) as min_resolution_hours,
                SUM(CASE 
                    WHEN messages IS NOT NULL 
                    THEN jsonb_array_length(messages) 
                    ELSE 0 
                END) as total_messages,
                COUNT(DISTINCT user_id) as unique_users
            FROM raw_data.support_tickets
            GROUP BY DATE(created_at), issue_type
        )
        SELECT 
            date,
            issue_type,
            total_tickets,
            open_tickets,
            closed_tickets,
            ROUND(COALESCE(avg_resolution_hours, 0)::numeric, 2) as avg_resolution_time_hours,
            ROUND(COALESCE(max_resolution_hours, 0)::numeric, 2) as max_resolution_time_hours,
            ROUND(COALESCE(min_resolution_hours, 0)::numeric, 2) as min_resolution_time_hours,
            total_messages,
            unique_users
        FROM ticket_stats
        ORDER BY date, issue_type;
    """
    
    cursor.execute(query)
    pg_conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM mart.support_efficiency")
    count = cursor.fetchone()[0]
    logger.info(f"Витрина support_efficiency заполнена: {count} записей")
    
    cursor.close()
    pg_conn.close()

def build_moderation_stats_mart(**context):
    logger.info("Начинаем построение витрины moderation_stats...")
    
    pg_conn = get_postgres_connection()
    cursor = pg_conn.cursor()
    
    cursor.execute("TRUNCATE mart.moderation_stats;")
    
    query = """
        INSERT INTO mart.moderation_stats 
        (date, moderation_status, total_reviews, avg_rating, total_flags, unique_products, unique_users)
        
        SELECT 
            DATE(submitted_at) as date,
            moderation_status,
            COUNT(*) as total_reviews,
            ROUND(AVG(rating)::numeric, 2) as avg_rating,
            COALESCE(SUM(array_length(flags, 1)), 0) as total_flags,
            COUNT(DISTINCT product_id) as unique_products,
            COUNT(DISTINCT user_id) as unique_users
        FROM raw_data.moderation_queue
        GROUP BY DATE(submitted_at), moderation_status
        ORDER BY date, moderation_status;
    """
    
    cursor.execute(query)
    pg_conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM mart.moderation_stats")
    count = cursor.fetchone()[0]
    logger.info(f"Витрина moderation_stats заполнена: {count} записей")
    
    cursor.close()
    pg_conn.close()

def build_product_popularity_mart(**context):
    logger.info("Начинаем построение витрины product_popularity...")
    
    pg_conn = get_postgres_connection()
    cursor = pg_conn.cursor()
    
    cursor.execute("TRUNCATE mart.product_popularity;")
    
    query = """
        INSERT INTO mart.product_popularity 
        (date, product_id, total_views, total_reviews, avg_rating, times_recommended)
        
        WITH product_views AS (
            SELECT 
                DATE(s.start_time) as date,
                unnest(pages_visited) as page,
                COUNT(*) as views
            FROM raw_data.user_sessions s
            WHERE pages_visited IS NOT NULL
            GROUP BY DATE(s.start_time), unnest(pages_visited)
        ),
        product_reviews AS (
            SELECT 
                DATE(submitted_at) as date,
                product_id,
                COUNT(*) as reviews,
                AVG(rating) as avg_rating
            FROM raw_data.moderation_queue
            WHERE moderation_status = 'approved'
            GROUP BY DATE(submitted_at), product_id
        ),
        product_recommendations AS (
            SELECT 
                DATE(last_updated) as date,
                unnest(recommended_products) as product_id,
                COUNT(*) as recommendations
            FROM raw_data.user_recommendations
            GROUP BY DATE(last_updated), unnest(recommended_products)
        )
        SELECT 
            COALESCE(v.date, r.date, rec.date) as date,
            COALESCE(
                regexp_replace(v.page, '^/products/', ''), 
                r.product_id, 
                rec.product_id
            ) as product_id,
            COALESCE(v.views, 0) as total_views,
            COALESCE(r.reviews, 0) as total_reviews,
            COALESCE(r.avg_rating, 0)::numeric(3,2) as avg_rating,
            COALESCE(rec.recommendations, 0) as times_recommended
        FROM product_views v
        FULL OUTER JOIN product_reviews r ON 
            v.date = r.date AND 
            regexp_replace(v.page, '^/products/', '') = r.product_id
        FULL OUTER JOIN product_recommendations rec ON 
            COALESCE(v.date, r.date) = rec.date AND
            COALESCE(
                regexp_replace(v.page, '^/products/', ''), 
                r.product_id
            ) = rec.product_id
        WHERE 
            COALESCE(
                regexp_replace(v.page, '^/products/', ''), 
                r.product_id, 
                rec.product_id
            ) IS NOT NULL
        ORDER BY date, product_id;
    """
    
    cursor.execute(query)
    pg_conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM mart.product_popularity")
    count = cursor.fetchone()[0]
    logger.info(f"Витрина product_popularity заполнена: {count} записей")
    
    cursor.close()
    pg_conn.close()

with DAG(
    'full_mongodb_to_postgres_replication',
    default_args=default_args,
    description='Full MongoDB to PostgreSQL replication with marts',
    schedule_interval='@hourly',
    catchup=False,
    tags=['mongodb', 'postgresql', 'replication'],
    max_active_runs=1,
) as dag:

    start_replication = DummyOperator(task_id='start_replication')
    
    replicate_sessions = PythonOperator(
        task_id='replicate_user_sessions',
        python_callable=replicate_user_sessions,
        retries=3
    )
    
    replicate_events = PythonOperator(
        task_id='replicate_event_logs',
        python_callable=replicate_event_logs,
        retries=3
    )
    
    replicate_tickets = PythonOperator(
        task_id='replicate_support_tickets',
        python_callable=replicate_support_tickets,
        retries=3
    )
    
    replicate_recommendations = PythonOperator(
        task_id='replicate_user_recommendations',
        python_callable=replicate_user_recommendations,
        retries=3
    )
    
    replicate_moderation = PythonOperator(
        task_id='replicate_moderation_queue',
        python_callable=replicate_moderation_queue,
        retries=3
    )
    
    replication_complete = DummyOperator(task_id='replication_complete')
    
    start_build_marts = DummyOperator(task_id='start_build_marts')
    
    build_user_activity = PythonOperator(
        task_id='build_user_activity_mart',
        python_callable=build_user_activity_mart
    )
    
    build_support_efficiency = PythonOperator(
        task_id='build_support_efficiency_mart',
        python_callable=build_support_efficiency_mart
    )
    
    build_moderation_stats = PythonOperator(
        task_id='build_moderation_stats_mart',
        python_callable=build_moderation_stats_mart
    )
    
    build_product_popularity = PythonOperator(
        task_id='build_product_popularity_mart',
        python_callable=build_product_popularity_mart
    )
    
    marts_complete = DummyOperator(task_id='marts_complete')
    
    start_replication >> [replicate_sessions, replicate_events, replicate_tickets, 
                          replicate_recommendations, replicate_moderation] >> replication_complete
    
    replication_complete >> start_build_marts
    
    start_build_marts >> [build_user_activity, build_support_efficiency, 
                          build_moderation_stats, build_product_popularity] >> marts_complete