import psycopg2
import pandas as pd

# from ElephantSQL
db = 'qksokbcl'
user = 'qksokbcl'
password = '*****'
host = 'otto.db.elephantsql.com'


def pg_connect(db=db, user=user, pw=password, host=host):
    return psycopg2.connect(dbname=db, user=user,
                            password=pw, host=host)


def pg_query(conn, query):
    """for SELECT queries"""
    crs = conn.cursor()
    crs.execute(query)
    return crs.fetchall()


def pg_ddl(conn, query):
    """for db modification (CREATE, INSERT)"""
    crs = conn.cursor()
    crs.execute(query)
    return


def san_quotes(string):
    return string.replace("'", "''")


def pg_cleanup(conn):
    crs = conn.cursor()
    query = """DROP TABLE IF EXISTS titanic;
    DROP TYPE IF EXISTS enum_class;
    DROP TYPE IF EXISTS enum_sex;"""
    crs.execute(query)
    return


if __name__ == '__main__':

    # csv -> python db
    df = pd.read_csv("./titanic.csv")

    # cleanup previous tests
    connection = pg_connect()
    pg_cleanup(connection)
    connection.commit()

    # create postgres table
    q_create_table = """CREATE TYPE  
    enum_class AS ENUM ('1', '2', '3');
    CREATE TYPE  
    enum_sex AS ENUM ('male', 'female');
    CREATE TABLE IF NOT EXISTS titanic (
        id_num SERIAL PRIMARY KEY,
        survived INT,
        pclass enum_class,
        name VARCHAR(96),
        sex enum_sex,
        age INT,
        sibs_spouses INT,
        parents_children INT,
        fare NUMERIC(8, 4)
        )"""
    pg_ddl(connection, q_create_table)
    connection.commit()

    # insert row data
    q_insert = """INSERT INTO titanic (
        survived,
        pclass,
        name,
        sex,
        age,
        sibs_spouses,
        parents_children,
        fare) 
        VALUES 
        """
    for index, row in df.iterrows():
        r_str = f"({row['Survived']}, '{row['Pclass']}',\
             '{san_quotes(row['Name'])}', '{row['Sex']}',\
                 {row['Age']}, {row['Siblings/Spouses Aboard']},\
                     {row['Parents/Children Aboard']}, {row['Fare']})"
        q_insert += r_str + ','
    q_insert = q_insert[:-1]  # remove trailing comma

    pg_ddl(connection, q_insert)
    connection.commit()
