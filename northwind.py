import sqlite3


def sql_connect(db='northwind_small.sqlite3'):
    """connect to local sqlite database"""
    return sqlite3.connect(db)


def exec_query(conn, query):
    """execute a data retrieval command"""
    crs = conn.cursor()
    crs.execute(query)
    return crs.fetchall()


# ***Queries Below***

expensive_items = """
SELECT *
FROM Product
ORDER BY UnitPrice DESC
LIMIT 10;
"""
# output
"""[(38, 'Côte de Blaye', 18, 1, '12 - 75 cl bottles', 263.5, 17, 0, 15, 0),\
 (29, 'Thüringer Rostbratwurst', 12, 6, '50 bags x 30 sausgs.',\
     123.79, 0, 0, 0, 1),\
 (9, 'Mishi Kobe Niku', 4, 6, '18 - 500 g pkgs.', 97, 29, 0, 0, 1),\
 (20, "Sir Rodney's Marmalade", 8, 3, '30 gift boxes', 81, 40, 0, 0, 0),\
 (18, 'Carnarvon Tigers', 7, 8, '16 kg pkg.', 62.5, 42, 0, 0, 0),\
 (59, 'Raclette Courdavault', 28, 4, '5 kg pkg.', 55, 79, 0, 0, 0),\
 (51, 'Manjimup Dried Apples', 24, 7, '50 - 300 g pkgs.', 53, 20, 0, 10, 0),\
 (62, 'Tarte au sucre', 29, 3, '48 pies', 49.3, 17, 0, 0, 0),\
 (43, 'Ipoh Coffee', 20, 1, '16 - 500 g tins', 46, 17, 10, 25, 0),\
 (28, 'Rössle Sauerkraut', 12, 7, '25 - 825 g cans', 45.6, 26, 0, 0, 1)]"""

# sqlite3 doesn't support DATEDIFF apparently
avg_hire_age = """
SELECT AVG(HireDate - BirthDate)
FROM Employee;
"""
# output
"""[(37.22222222222222,)]"""

ten_most_expensive = """
SELECT ProductName, UnitPrice, CompanyName
FROM Product
JOIN Supplier
ON Product.SupplierId = Supplier.Id
ORDER BY UnitPrice DESC
LIMIT 10;
"""
# output
"""[('Côte de Blaye', 263.5, 'Aux joyeux ecclésiastiques'),\
 ('Thüringer Rostbratwurst', 123.79, 'Plutzer Lebensmittelgroßmärkte AG'),\
 ('Mishi Kobe Niku', 97, 'Tokyo Traders'),\
 ("Sir Rodney's Marmalade", 81, 'Specialty Biscuits, Ltd.'),\
 ('Carnarvon Tigers', 62.5, 'Pavlova, Ltd.'),\
 ('Raclette Courdavault', 55, 'Gai pâturage'),\
 ('Manjimup Dried Apples', 53, "G'day, Mate"),\
 ('Tarte au sucre', 49.3, "Forêts d'érables"),\
 ('Ipoh Coffee', 46, 'Leka Trading'),\
 ('Rössle Sauerkraut', 45.6, 'Plutzer Lebensmittelgroßmärkte AG')]"""

largest_category = """
SELECT Category.CategoryName, COUNT(Product.Id)
FROM Product
JOIN Category
ON Product.CategoryId = Category.Id
GROUP BY Category.CategoryName
ORDER BY COUNT(Product.Id) DESC
LIMIT 1;
"""
# output
"""[('Confections', 13)]"""

if __name__ == "__main__":
    conn = sql_connect()

    print("expensive_items")
    print(exec_query(conn, expensive_items))

    print("avg_hire_age")
    print(exec_query(conn, avg_hire_age))

    print("ten_most_expensive")
    print(exec_query(conn, ten_most_expensive))

    print("largest_category")
    print(exec_query(conn, largest_category))
