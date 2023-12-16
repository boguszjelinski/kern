import sys # to access the system
import cv2
import time
import mysql.connector
import sys
import random

YELLOW = (0, 200, 255)
BLACK = (0, 0, 0)
RED = (0, 0, 255)
DARKRED = (0, 0, 150)
BLUE = (255, 0, 0)
GREEN = (0, 120, 0)

def draw_img():
    global img
    global x_loc
    global w
    global magnification
    global y_loc
    global h
    global win_size
    crop_img = img[y_loc: y_loc + int(h/magnification), x_loc: x_loc + int(w/magnification)]
    imS = cv2.resize(crop_img, (win_size, win_size))
    cv2.imshow(winname, imS)  

def show_db(object):
    global img
    global img_orig
    global win_size
    global winname
    img = img_orig.copy()
    conn = mysql.connector.connect(host='localhost', user='kabina', password='kaboot', database='kabina', auth_plugin='mysql_native_password') #192.168.10.176 6432
    cursor = conn.cursor()
    sql = ""
    if object == 'ORDER':
        sql = "select longitude, latitude, status from taxi_order as c, stop as s where s.id = from_stand"
    elif object == 'CAB':
        sql = "select longitude, latitude, status from cab as c, stop as s where s.id = location"
    else:
        sql = "select longitude, latitude FROM stop"
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        x = random.randint(-4, 4) + (row[0] - min_lon) * pix_per_lon;
        y = random.randint(-4, 4) + (row[1] - min_lat) * pix_per_lat;
        center_coordinates = (int(x), int(h-y)) 
        radius = 15
        color = RED
        if object == 'CAB':
            if row[2] == 0: # ASSIGNED
                color = YELLOW
            elif row[2] == 1: # FREE
                color = BLACK
            # else out-of-service RED
        elif object == 'ORDER':
            if row[2] == 0: # RECEIVED
                color = DARKRED
            elif row[2] == 1: # ASSIGNED
                color = BLACK
            elif row[2] == 7: # picked up
                color = YELLOW
            # else any other RED
        elif object == 'STOP':
            color = BLUE
        thickness = 10
        img = cv2.circle(img, center_coordinates, radius, color, thickness) 
        
    imS = cv2.resize(img, (win_size, win_size))
    cv2.imshow(winname, imS)   

def show_route(ord_id):
    global img
    global img_orig
    global win_size
    global winname
    img = img_orig.copy()
    conn = mysql.connector.connect(host='localhost', user='kabina', password='kaboot', database='kabina', auth_plugin='mysql_native_password') #192.168.10.176 6432
    cursor = conn.cursor()
    sql = "select l.from_stand, l.to_stand, l.status, o.from_stand, o.to_stand, \
            (SELECT longitude FROM STOP WHERE id=l.from_stand),  \
            (SELECT latitude FROM STOP WHERE id=l.from_stand),\
            (SELECT longitude FROM STOP WHERE id=l.to_stand),  \
            (SELECT latitude FROM STOP WHERE id=l.to_stand)\
            from leg as l, taxi_order as o where o.id = " + str(ord_id) + " AND l.route_id = o.route_id ORDER by place"
    cursor.execute(sql)
    rows = cursor.fetchall()
    prev_x = -1
    prev_y = -1
    to_x = -1
    to_y = -1
    last_to = -1
    order_to = -1
    prev_status = -1

    for row in rows:
        x = int((row[5] - min_lon) * pix_per_lon);
        y = int((row[6] - min_lat) * pix_per_lat);

        # STOP
        center_coordinates = (int(x), int(h - y)) 
        radius = 15
        color = BLUE
        if row[0] == row[3] or row[0] == row[4]: # pick-up or drop-off of the passenger
            color = RED
        thickness = 10
        img = cv2.circle(img, center_coordinates, radius, color, thickness) 

        # ARROW
        if prev_x != -1: # not the first stop
            if prev_status == 1: # ASSIGNED
                color = RED
            elif prev_status == 5: # STARTED
                color = GREEN
            elif prev_status == 6: # COMPLETED
                color = BLACK
            else:
                color = BLUE

            start_point = (prev_x, h - prev_y)  
            end_point = (x, h - y)  
            thickness = 9
            img = cv2.arrowedLine(img, start_point, end_point, color, thickness)  

        prev_x = x
        prev_y = y
        # these will be used to draw the last leg after the loop finished
        to_x = int((row[7] - min_lon) * pix_per_lon);
        to_y = int((row[8] - min_lat) * pix_per_lat);
        last_to = row[1]
        order_to = row[4]
        prev_status = row[2]

    # STOP
    center_coordinates = (int(to_x), int(h - to_y)) 
    radius = 15
    color = (255, 0, 0) # BLUE
    if last_to == order_to: # pick-up or drop-off of the passenger
        color = (0, 0, 255) # RED
    thickness = 10
    img = cv2.circle(img, center_coordinates, radius, color, thickness) 

    # ARROW
    if prev_status == 1: # ASSIGNED
        color = RED
    elif prev_status == 5: # STARTED
        color = GREEN
    elif prev_status == 6: # COMPLETED
        color = BLACK
    else:
        color = BLUE

    start_point = (prev_x, h - prev_y)  
    end_point = (to_x, h - to_y)  
    thickness = 9
    img = cv2.arrowedLine(img, start_point, end_point, color, thickness) 
    imS = cv2.resize(img, (win_size, win_size))
    cv2.imshow(winname, imS)   

# GLOBAL VALUES
maks_lat = 47.673487 
maks_lon = 19.375611
min_lat = 47.326478
min_lon = 18.864488
lat_diff = maks_lat - min_lat
lon_diff = maks_lon - min_lon
pkt_lat = 47.507803
pkt_lon = 19.235276 # Erdei bekötőút
w = 11928
h = 12000
win_size = 1500
pix_per_lon = w / lon_diff;
pix_per_lat = h / lat_diff;
magnification = 1
x_loc = 0
y_loc = 0

# START PROGRAME
ord_id = -1
if len(sys.argv) > 1:
    ord_id = int(sys.argv[1])

img = cv2.imread("/Users/m91127/TAXI/budapest-openstreet.png", cv2.IMREAD_ANYCOLOR)
img_orig = img.copy()
winname = "Kabina viewer - press 'h'for help"
cv2.namedWindow(winname)

view = 'ORDER'
if ord_id == -1:
    show_db(view)
else: 
    show_route(ord_id)

while True:
    k = cv2.waitKey(0)
    print('Key: ', k)
    if k == 27 : # UP: 0, DOWN: 1, RIGHT: 3, LEFT: 2, MINUS: 45, PLUS: 43
        break
    elif k == 43 and magnification < 16 : # PLUS
        magnification *= 2
        draw_img()  
    elif k == 45 and magnification > 1 : # MINUS
        # check if not at the edge
        if y_loc + int(h/magnification) + 1 > h:
            y_loc -= int(h/magnification)
        if x_loc + int(w/magnification) + 1 > w:
            x_loc -= int(w/magnification)            
        magnification = int(magnification/2)
        print('magn: ', magnification)
        draw_img()
    elif k == 3 and x_loc + int(w/magnification) < w - int(w/magnification) + 1: # RIGHT
        x_loc += int(w/magnification)
        print('x_loc: ', x_loc)
        draw_img()  
    elif k == 2 : # LEFT
        if x_loc - int(w/magnification) < 0:
            x_loc = 0
        else:
            x_loc -= int(w/magnification)
        print('x_loc: ', x_loc)
        draw_img()
    elif k == 1 and y_loc + int(h/magnification) < h - int(h/magnification) + 1: # DOWN
        y_loc += int(h/magnification)
        print('y_loc: ', y_loc)
        draw_img()  
    elif k == 0 : # UP
        if y_loc - int(h/magnification) < 0:
            y_loc = 0
        else:
            y_loc -= int(h/magnification)
        print('y_loc: ', y_loc)
        draw_img()
    elif k == 104 : # h
        cv2.namedWindow("Help")
        img_help = cv2.imread("/Users/m91127/TAXI/viewer-help.png", cv2.IMREAD_ANYCOLOR)
        cv2.imshow("Help", img_help)
    elif k == 49 : # 1: ORDERS
        view = 'ORDER'
        show_db(view)
    elif k == 50 : # 2: CABS
        view = 'CAB'
        show_db(view)
    elif k == 51 : # 3: STOPS
        view = 'STOP'
        show_db(view)
    elif k == 32: # REREAD from database
        if ord_id == -1:
            show_db(view)
        else: 
            show_route(ord_id)
cv2.destroyAllWindows() # destroy all windows
sys.exit() 
