import time
import psycopg2
from concurrent.futures import ThreadPoolExecutor
import logging


logging.basicConfig(
    filename='updates.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DB_CONFIG = {
    'dbname': 'labs',
    'user': 'postgres',
    'password': '5656',
    'host': 'localhost',
    'port': 5432
}

def reset_database():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM user_counter WHERE user_id = 1")
            cur.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0)")
            conn.commit()

def describe_database():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, counter, version FROM user_counter WHERE user_id = 1")
            row = cur.fetchone()
            if row:
                logging.info(f"USER_ID: {row[0]}, Counter: {row[1]}, Version: {row[2]}")
            else:
                logging.info("Record with user_id=1 not found.")

def lost_update():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for _ in range(10_000):
                cur.execute("BEGIN")

                cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
                row = cur.fetchone()
                current_value = row[0]

                new_value = current_value + 1

                cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (new_value, 1))
                conn.commit()

    except Exception as e:
        logging.error(f"lost_update error: {e}")
        conn.rollback()
    finally:
        conn.close()

def in_place_update():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for _ in range(10_000):
                cur.execute("BEGIN")
                cur.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1")
                conn.commit()

    except Exception as e:
        logging.error(f"in_place_update error: {e}")
        conn.rollback()
    finally:
        conn.close()

def row_lock_update():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for _ in range(10_000):
                cur.execute("BEGIN")

                cur.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
                row = cur.fetchone()
                current_value = row[0]

                new_value = current_value + 1
                cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (new_value,))
                conn.commit()

    except Exception as e:
        logging.error(f"row_lock_update error: {e}")
        conn.rollback()
    finally:
        conn.close()

def optimistic_update():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for _ in range(10_000):
                while True:
                    cur.execute("BEGIN")

                    cur.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
                    row = cur.fetchone()
                    current_counter = row[0]
                    current_version = row[1]

                    new_counter = current_counter + 1
                    new_version = current_version + 1
                    cur.execute("""
                        UPDATE user_counter
                           SET counter = %s,
                               version = %s
                         WHERE user_id = 1
                           AND version = %s
                    """, (new_counter, new_version, current_version))

                    if cur.rowcount > 0:
                        conn.commit()
                        break
                    else:
                        conn.rollback()

    except Exception as e:
        logging.error(f"optimistic_update error: {e}")
        conn.rollback()
    finally:
        conn.close()

def main():
    thread_count = 10
    increments_per_thread = 10_000
    total_expected = thread_count * increments_per_thread

    # 1) Lost Update
    reset_database()
    logging.info(f"[Lost Update] Starting {thread_count} threads, each performing {increments_per_thread} increments.")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [executor.submit(lost_update) for _ in range(thread_count)]
        for f in futures:
            f.result()

    end_time = time.time()
    logging.info(f"[Lost Update] Completed in {end_time - start_time:.2f} seconds.")
    logging.info(f"[Lost Update] Expected final counter value: {total_expected}")
    describe_database()

    # 2) In-place Update
    reset_database()
    logging.info(f"\n[In-place Update] Starting {thread_count} threads, each performing {increments_per_thread} increments.")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [executor.submit(in_place_update) for _ in range(thread_count)]
        for f in futures:
            f.result()

    end_time = time.time()
    logging.info(f"[In-place Update] Completed in {end_time - start_time:.2f} seconds.")
    logging.info(f"[In-place Update] Expected final counter value: {total_expected}")
    describe_database()

    # 3) Row-level Locking
    reset_database()
    logging.info(f"\n[Row-level Locking] Starting {thread_count} threads, each performing {increments_per_thread} increments.")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [executor.submit(row_lock_update) for _ in range(thread_count)]
        for f in futures:
            f.result()

    end_time = time.time()
    logging.info(f"[Row-level Locking] Completed in {end_time - start_time:.2f} seconds.")
    logging.info(f"[Row-level Locking] Expected final counter value: {total_expected}")
    describe_database()

    # 4) Optimistic Concurrency Control
    reset_database()
    logging.info(f"\n[Optimistic Concurrency] Starting {thread_count} threads, each performing {increments_per_thread} increments.")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [executor.submit(optimistic_update) for _ in range(thread_count)]
        for f in futures:
            f.result()

    end_time = time.time()
    logging.info(f"[Optimistic Concurrency] Completed in {end_time - start_time:.2f} seconds.")
    logging.info(f"[Optimistic Concurrency] Expected final counter value: {total_expected}")
    describe_database()

if __name__ == "__main__":
    main()
