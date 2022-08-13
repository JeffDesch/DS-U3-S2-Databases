import sqlite3


def sql_connect(db='demo_data.sqlite3'):
    """connect to local sqlite database"""
    return sqlite3.connect(db)


def exec_query(conn, query):
    """execute a data retrieval command"""
    crs = conn.cursor()
    crs.execute(query)
    return crs.fetchall()


def exec_ddl(conn, query):
    """execute a table manipulation command"""
    crs = conn.cursor()
    crs.execute(query)
    conn.commit()
    return


def create_demo_table():
    """wrapper to create the demo table"""
    conn = sql_connect()

    reset_demo_db = """
    DROP TABLE IF EXISTS demo;
    """
    exec_ddl(conn, reset_demo_db)

    create_table = """
    CREATE TABLE IF NOT EXISTS demo (
        s VARCHAR(1),
        x INTEGER,
        y INTEGER
    );
    """
    exec_ddl(conn, create_table)

    insert_demo_data = """
    INSERT INTO demo
    (s, x, y)
    VALUES
    ('g', 3, 9),
    ('v', 5, 7),
    ('f', 8, 7);
    """
    exec_ddl(conn, insert_demo_data)
    return


# Results from queries (below):
row_count = [(3,)]
xy_at_least_5 = [(2,)]
unique_y = [(2,)]

if __name__ == "__main__":
    create_demo_table()
    conn = sql_connect()

    rcnt_query = """
    SELECT COUNT(*)
    FROM demo;
    """
    row_count = exec_query(conn, rcnt_query)
    print(row_count)

    xy_gt_5_query = """
    SELECT COUNT(*)
    FROM demo
    WHERE x >= 5 AND y >= 5;
    """
    xy_at_least_5 = exec_query(conn, xy_gt_5_query)
    print(xy_at_least_5)

    uni_y_query = """
    SELECT COUNT(DISTINCT y)
    FROM demo;
    """
    unique_y = exec_query(conn, uni_y_query)
    print(unique_y)
