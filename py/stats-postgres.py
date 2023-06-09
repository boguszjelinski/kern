import psycopg2
import time

def execSql(cursor, fil, sql, column):
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print(row[column], end = ', ', file=fil, flush=True)
    print('', file=fil, flush=True)

try:
    conn = psycopg2.connect("host=localhost port=5432 dbname=kabina user=kabina password=kaboot") #192.168.10.176 6432
    cur = conn.cursor()
    file1 = open('kpis.txt', 'w')
    file2 = open('cab_status.txt', 'w')
    file3 = open('order_status.txt', 'w')
    # column names
    execSql(cur, file1, 'select * from stat order by name', 0)
    
    for t in range(0,90):
        execSql(cur, file1, 'select * from stat order by name', 1)
        execSql(cur, file2, 'select status, count(*) from cab group by status order by status', 0)
        execSql(cur, file2, 'select status, count(*) from cab group by status order by status', 1)
        execSql(cur, file3, 'select status, count(*) from taxi_order group by status order by status', 0)
        execSql(cur, file3, 'select status, count(*) from taxi_order group by status order by status', 1)
        time.sleep(60) # 60 seconds
    file1.close()
    file2.close()
    file3.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
