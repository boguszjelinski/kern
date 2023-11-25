import mysql.connector
import time

def execSql(cursor, fil, sql, column):
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print(row[column], end = ', ', file=fil, flush=True)
    print('', file=fil, flush=True)

conn = mysql.connector.connect(host='localhost', user='kabina', password='kaboot', database='kabina', auth_plugin='mysql_native_password') #192.168.10.176 6432
cur = conn.cursor()
file1 = open('kpis.txt', 'w')
file2 = open('cab_status.txt', 'w')
file3 = open('order_status.txt', 'w')
# column names
execSql(cur, file1, 'select * from stat order by name', 0)

for t in range(0,120):
    execSql(cur, file1, 'select * from stat order by name', 1)
    execSql(cur, file2, 'select status, count(*) from cab group by status order by status', 0)
    execSql(cur, file2, 'select status, count(*) from cab group by status order by status', 1)
    execSql(cur, file3, 'select status, count(*) from taxi_order group by status order by status', 0)
    execSql(cur, file3, 'select status, count(*) from taxi_order group by status order by status', 1)
    time.sleep(60) # 60 seconds
file1.close()
file2.close()
file3.close()
conn.close()
