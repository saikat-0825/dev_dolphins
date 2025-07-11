import psycopg2

conn = psycopg2.connect(
    dbname='mechanism_y_db',
    user='postgres',
    password='admin',
    host='localhost',
    port='5432'
)

print(" Connected successfully!")
conn.close()