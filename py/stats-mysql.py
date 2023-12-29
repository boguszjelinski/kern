import mysql.connector
import time

def execSql(conn, fil, sql, column):
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print(row[column], end = ', ', file=fil, flush=True)
    print('', file=fil, flush=True)

conn = mysql.connector.connect(host='localhost', user='kabina', password='kaboot', database='kabina', auth_plugin='mysql_native_password') #192.168.10.176 6432
file1 = open('kpis.txt', 'w')
file2 = open('cab_status.txt', 'w')
file3 = open('order_status.txt', 'w')
# column names
execSql(conn, file1, 'select * from stat order by name', 0)

for t in range(0,120):
    conn = mysql.connector.connect(host='localhost', user='kabina', password='kaboot', database='kabina', auth_plugin='mysql_native_password') #192.168.10.176 6432
    execSql(conn, file1, 'select * from stat order by name', 1)
    execSql(conn, file2, 'select status, count(*) from cab group by status order by status', 0)
    execSql(conn, file2, 'select status, count(*) from cab group by status order by status', 1)
    execSql(conn, file3, 'select status, count(*) from taxi_order group by status order by status', 0)
    execSql(conn, file3, 'select status, count(*) from taxi_order group by status order by status', 1)
    conn.close()
    time.sleep(60) # 60 seconds
file1.close()
file2.close()
file3.close()
conn.close()
