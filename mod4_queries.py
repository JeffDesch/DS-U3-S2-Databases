from numpy import char
import psycopg2
import pymongo
import pprint

def pg_connect(db='qksokbcl', user='qksokbcl',
               pw='******',
               host='otto.db.elephantsql.com'):
    """connect to PostgreSQL db via ElephantSQL"""
    return psycopg2.connect(dbname=db, user=user,
                            password=pw, host=host)

def pg_query(conn, query):
    """for SELECT postgre queries"""
    crs = conn.cursor()
    crs.execute(query)
    return crs.fetchall()

def mongo_connect(db='test', collection='rpg_db', password='*****'):
    """establish MongoDB connection"""
    conn_str = "mongodb+srv://JDesch:" + password +\
        "@jdcluster.wgpftmd.mongodb.net/?retryWrites=true&w=majority"
    mongo_client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    try:
        #print(mongo_client.server_info())
        print("Connected to Mongo server instance")
    except Exception:
        print("Unable to connect to the server.")
    mdb = mongo_client[db]  # database
    mcl = mdb[collection]  # collection
    return mcl

def mongo_find(db_col, query):
    """for simple filter queries"""
    return db_col.find(query)

def mongo_agg(db_col, agg_query):
    """for aggregation based queries"""
    return db_col.aggregate(agg_query)


if __name__ == '__main__':
    #***PART ONE - PostgreSQL Titanic Data***
    pg_conn = pg_connect()
    print("***PART ONE - PostgreSQL Titanic Data***")

    # How many passengers survived, and how many died?
    query="""SELECT survived, COUNT(*) FROM titanic
     GROUP BY survived 
     ORDER BY survived DESC
    """
    print("How many passengers survived, and how many died?")
    pprint(pg_query(pg_conn, query))

    # How many passengers were in each class?
    query="""SELECT pclass, COUNT(*)
     FROM titanic
     GROUP BY pclass
     ORDER BY pclass ASC
    """
    print("How many passengers were in each class?")
    pprint(pg_query(pg_conn, query))

    # How many passengers survived/died within each class?
    query="""SELECT survived, pclass, COUNT(*)
     FROM titanic
     GROUP BY (survived, pclass)
     ORDER BY (pclass, survived)
    """
    print("How many passengers survived/died within each class?")
    pprint(pg_query(pg_conn, query))

    # What was the average age of survivors vs nonsurvivors?
    query="""SELECT survived, AVG(age)
     FROM titanic
     GROUP BY survived 
    """
    print("What was the average age of survivors vs nonsurvivors?")
    pprint(pg_query(pg_conn, query))

    # What was the average age of each passenger class?
    query="""SELECT pclass, AVG(age)
     FROM titanic
     GROUP BY pclass
     ORDER BY pclass
    """
    print("What was the average age of each passenger class?")
    pprint(pg_query(pg_conn, query))

    # What was the average fare by passenger class? By survival?
    query="""SELECT AVG(fare), pclass, survived
     FROM titanic
     GROUP BY (pclass, survived)
     ORDER BY (pclass, survived)
    """
    print("What was the average fare by passenger class? By survival?")
    pprint(pg_query(pg_conn, query))

    # How many siblings/spouses aboard on average, by passenger class? By survival?
    query="""SELECT AVG(sibs_spouses), pclass, survived
     FROM titanic
     GROUP BY (pclass, survived)
     ORDER BY (pclass, survived)
    """
    print("How many siblings/spouses aboard on average, by passenger class? By survival?")
    pprint(pg_query(pg_conn, query))

    # How many parents/children aboard on average, by passenger class? By survival?
    query="""SELECT AVG(parents_children), pclass, survived
     FROM titanic
     GROUP BY (pclass, survived)
     ORDER BY (pclass, survived)
    """
    print("How many parents/children aboard on average, by passenger class? By survival?")
    pprint(pg_query(pg_conn, query))

    # Do any passengers have the same name?
    query="""SELECT name, COUNT(name)
     FROM titanic 
     GROUP BY name 
     HAVING COUNT(name) > 1
    """
    print("Do any passengers have the same name?")
    pprint(pg_query(pg_conn, query))


    #***PART TWO - MongoDB RPG Data***
    mgo_db = mongo_connect()
    print("***PART TWO - MongoDB RPG Data***")

    # How many total Characters are there?
    query="""{}"""
    print("How many total Characters are there?")
    char_count = mgo_db.count_documents(query)
    print(char_count)

    # How many total Items?
    query="""{$unwind : "$items" }"""
    print("How many total Items?")
    item_count = mgo_db.count_documents(query)
    print(item_count)

    # How many of the Items are Weapons? How many are not?
    query="""{$unwind : "$weapons" }"""
    weapon_count = mgo_db.count_documents(query)
    print("How many of the Items are Weapons? How many are not?")
    print("Weapons: ", weapon_count)
    print("Non-weapons: ", item_count - weapon_count)

    # How many Items does each Character have? (Return first 20 rows)
    query="""[
        {$unwind: "$items"}
        {
        "$group": {
            "_id": "character_id",
            "item_count: {$sum:1}
        }
    }]
    """
    print("How many Items does each Character have?")
    for doc in mgo_db.aggregate(query).limit(20):
        pprint(doc)

    # How many Weapons does each Character have? (Return first 20 rows)
    query=""""[
        {$unwind: "$weapons"}
        {
        "$group": {
            "_id": "character_id",
            "weapon_count: {$sum:1}
        }
    }]
    """
    print("How many Weapons does each Character have?")
    for doc in mgo_db.aggregate(query).limit(20):
        pprint(doc)

    # On average, how many Items does each Character have?
    print("On average, how many Items does each Character have?")
    print(item_count / char_count)

    # On average, how many Weapons does each Character have?
    print("On average, how many Weapons does each Character have?")
    print(weapon_count / char_count)