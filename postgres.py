import psycopg2
from tabulate import tabulate

con = psycopg2.connect(
    host="localhost",
    database="production",
    user="postgres",
    password="*****")



#For isolation: SERIALIZABLE
con.set_isolation_level(3)
#For atomicity
con.autocommit = False

try:
    cur = con.cursor()
    depot_headers = ["dep", "addr", "volume"]
    product_headers = ["prod", "pname", "price"]
    stock_headers = ["prod", "dep", "quantity"]
    query1 = ("select * from product")
    query2 = ("select * from depot")
    query3 = ("select * from stock")
    query4 = ("delete from stock where dep='d1'")
    query5 = ("delete from depot where dep='d1'")

    #Delete d1 from stock and depot
    cur.execute(query4)
    cur.execute(query5)

    cur.execute(query1)
    product = cur.fetchall()
    print("Product Table")
    print(tabulate(product, product_headers, "psql"))
    cur.execute(query2)
    depot = cur.fetchall()
    print("Depot Table")
    print(tabulate(depot, depot_headers, "psql"))
    cur.execute(query3)
    stock = cur.fetchall()
    print("Stock Table")
    print(tabulate(stock, stock_headers, "psql"))
    print("Transaction successful")

except (Exception, psycopg2.DatabaseError) as err:
    print(err)
    print("Transactions could not be completed so database will be rolled back before start of transactions")
    con.rollback()
finally:
    if con:
        con.commit()
        cur.close
        con.close
        print("PostgreSQL connection is now closed")