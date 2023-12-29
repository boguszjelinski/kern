import mysql.connector
import time

conn = mysql.connector.connect(host='localhost', user='kabina', password='kaboot', database='kabina', auth_plugin='mysql_native_password') #192.168.10.176 6432
curs = conn.cursor()
curs.execute("SELECT distinct l.route_id FROM leg as l, leg as l2 WHERE l.route_id=l2.route_id \
                     AND l.id != l2.id AND l.from_stand=l2.from_stand ORDER by l.route_id")
# don't check routes with duplicated stands, these are hard to check
omit_routes = [i[0] for i in curs.fetchall()]

# if passengers er counted correctly
cursor = conn.cursor()
cursor.execute("SELECT l.id, l.route_id, l.place, l.from_stand, l.passengers, o.from_stand, o.to_stand, o.id \
               FROM leg as l, taxi_order as o \
                WHERE o.route_id = l.route_id AND (o.from_stand = l.from_stand OR o.to_stand = l.from_stand) \
                ORDER by l.route_id, l.place")
rows = cursor.fetchall()
prev_route_id = -1
prev_passengers = 0
prev_leg = -1
prev_place = 0
passenger_count = 0
orders = []
for row in rows:
    if prev_route_id not in omit_routes and row[2] != prev_place and prev_passengers < passenger_count:
        print('Leg {}, route {}, place {} had more passengers, {} > {}'.format(prev_leg, prev_route_id, prev_place, passenger_count, prev_passengers))
    if row[1] != prev_route_id: # new route
        passenger_count = 0
        prev_route_id = row[1]
        orders = []
    if row[3] == row[5] and row[7] not in orders: # pick-up; 'not in' as there can be duplicates and this customer might be already dropped off  
        passenger_count += 1
        orders.append(row[7])
    if row[3] == row[6] and row[7] in orders: # drop-off, this check is needed as a route can have duplicated stops
        passenger_count -= 1
    prev_passengers = row[4]
    prev_leg = row[0]
    prev_place = row[2]
conn.close()
