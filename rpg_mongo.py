import pymongo
import sqlite3


def mongo_connect(db='test', collection='rpg_db', password='*****'):
    """establish MongoDB connection"""
    conn_str = "mongodb+srv://JDesch:" + password +\
        "@jdcluster.wgpftmd.mongodb.net/?retryWrites=true&w=majority"
    mongo_client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    try:
        print(mongo_client.server_info())
    except Exception:
        print("Unable to connect to the server.")
    mdb = mongo_client[db]  # database
    mcl = mdb[collection]  # collection
    return mcl


def sql_connect(local_db='rpg_db.sqlite3'):
    """ connect to local sqlite db"""
    sqlconn = sqlite3.connect(local_db)
    return sqlconn


def exec_sql_query(conn, query):
    """executes sqlite query"""
    crs = conn.cursor()
    crs.execute(query)
    return crs.fetchall()


def gen_char_docs(conn, char_data):
    """generates dictionaries from rpg_db character table"""
    for row in char_data:
        doc = {
            'character_id': row[0],
            'name': row[1],
            'level': row[2],
            'exp': row[3],
            'hp': row[4],
            'strength': row[5],
            'intelligence': row[6],
            'dexterity': row[7],
            'wisdom': row[8],
            'items': make_item_docs(conn, row[0], False),
            'weapons': make_item_docs(conn, row[0], True)
        }
        yield doc
    


def make_item_docs(conn, char_id, weapon=False):
    """subroutine to get items for a given character"""
    item_docs = []
    if weapon:
        item_query = f"""SELECT w.*
FROM charactercreator_character_inventory as ci
JOIN armory_weapon as w
	ON ci.item_id = w.item_ptr_id
WHERE ci.character_id = {char_id}"""
        item_data = exec_sql_query(conn, item_query)
        for item_row in item_data:
            item_doc = {
                'item_ptr_id': item_row[0],
                'power': item_row[1]
            }
            item_docs.append(item_doc)
    else:
        item_query = f"""
SELECT i.*
FROM charactercreator_character_inventory as ci
JOIN armory_item as i
	ON ci.item_id = i.item_id
WHERE ci.character_id = {char_id}
        """
        item_data = exec_sql_query(conn, item_query)
        item_docs = []
        for item_row in item_data:
            item_doc = {
                'item_id': item_row[0],
                'name': item_row[1],
                'value': item_row[2],
                'weight': item_row[3],
            }
            item_docs.append(item_doc)

    return item_docs


if __name__ == '__main__':
    all_chars = """SELECT * FROM charactercreator_character;"""
    rpg_db = mongo_connect()
    sql_conn = sql_connect()
    char_data = exec_sql_query(sql_conn, all_chars)

    for char_doc in gen_char_docs(sql_conn, char_data):
        rpg_db.insert_one(char_doc)