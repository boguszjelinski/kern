import psycopg2
import time
import math
from tkinter import * 

id = [-1 for i in range(5000)] 
routeId = [-1 for i in range(5000)] 
place = [-1 for i in range(5000)] 
fromStand = [-1 for i in range(5000)] 
toStand = [-1 for i in range(5000)] 
distance = [-1 for i in range(5000)] 
started = [-1 for i in range(5000)] 
completed = [-1 for i in range(5000)] 
#
stopId = [-1 for i in range(6000)] 
bearing = [-1 for i in range(6000)] 
longitude = [-1 for i in range(6000)] 
latitude = [-1 for i in range(6000)] 
name = [-1 for i in range(6000)] 
#
min_long = 18.881264 
max_long = 19.357892
min_lat = 47.338022 # select min(latitude) from stop
max_lat = 47.659618
margin = 50
routeIdx = 1
numbStops = 0
numbRoutes = 0
numbLegs = 0

root = Tk() 
root.geometry("2100x1100")
root.title("Route presenter")
canvas = Canvas(root, width=2100, height=1100, bg='white')

def getX(long):
    temp_x = (long - min_long) / (max_long - min_long)
    return margin + (2100 - 2*margin) * temp_x

def getY(lat):
    temp_y = (lat - min_lat) / (max_lat - min_lat)
    return 1000 - margin - (1100 - 2*margin) * temp_y

def draw_circle(x, y, r, cnvs): 
    x0 = x - r
    y0 = y - r
    x1 = x + r
    y1 = y + r
    return cnvs.create_oval(x0, y0, x1, y1)

def draw_bearing (x,y, rad, angle, canvas):
    radius = 15
    x1 = x + rad *math.sin(math.radians(angle))
    y1 = y - rad *math.cos(math.radians(angle))
    x2 = x + radius *math.sin(math.radians(angle))
    y2 = y - radius *math.cos(math.radians(angle))
    canvas.create_line(x1, y1, x2, y2, width=2, fill='red')

def onKeyPress(event):
    global routeIdx
    global numbRoutes

    if event.keysym == "Up":
        if routeIdx > 1:
            routeIdx -= 1
    else:
        if routeIdx < numbRoutes - 1: 
            routeIdx += 1
    showRoute(routeIdx)

def show_orders(route_id, canvas):
    global conn
    width = [70, 50, 50, 50, 50, 50, 65, 75, 100, 100, 100]
    label = ['ID', 'FROM', 'TO', 'WAIT', 'LOSS', 'DIST', 'LEG', 'ETA', 'RCVD', 'STARTED', 'CMPLTED']
    try:
        cur = conn.cursor()
        cur.execute(("select id, from_stand, to_stand, max_wait, max_loss, distance, leg_id, eta, received, started, completed FROM taxi_order WHERE route_id=%s ORDER BY leg_id" % route_id))
        rows = cur.fetchall()
        offset = 1320
        y = 20
        canvas.create_text(offset, y, font="Console 13 bold", text='Orders:')
        offset = offset + 70
        for i in range(11):
            canvas.create_text(offset, y, font="Console 10 bold", text=label[i])
            offset = offset + width[i]
        y = y + 15
        for row in rows:
            offset = 1390
            for i in range(11):
                if (i>7):
                    canvas.create_text(offset, y, font="Console 10 bold", text=str(row[i])[11:19])
                else:
                    canvas.create_text(offset, y, font="Console 10 bold", text=row[i])
                offset = offset + width[i]
            y = y + 15
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def showRoute(idx):
    global canvas
    global numbLegs
    global longitude
    global latitude
    global fromStand
    width = [50, 50, 50, 75, 100, 100]
    label = ['#', 'FROM', 'TO', 'DST', 'STRTED', 'COMPLTD']
    y = 685
    canvas.delete("all")
    offset = 1720
    canvas.create_text(offset + 20, y, font="Console 13 bold", text='Legs:')
    y = y + 25
    for i in range(6):
        canvas.create_text(offset, y, font="Console 10 bold", text=label[i])
        offset = offset + width[i]

    y = y + 15
    place = 0
    found = 0
    for i in range(numbLegs-1):
        if id[i] != idx and found == 1:
            # last leg
            show_circle(toStand[i - 1], place, canvas)
            break
        if id[i] == idx:
            if found == 0:
                found = 1
                # route ID in the top left corner
                canvas.create_text(50, 10, font="Console 10 bold",text=str(routeId[i]))
                show_orders(routeId[i], canvas)
            show_circle(fromStand[i], place, canvas)
            show_leg(i, place, 1720, y, width, canvas)
            place += 1
            y = y + 15

    # show legs        

def show_leg (idx, place, offset, y, width, canvas):
    canvas.create_text(offset, y, font="Console 10 bold", text=str(place))
    offset = offset + width[0]
    canvas.create_text(offset, y, font="Console 10 bold", text=str(fromStand[idx]))
    offset = offset + width[1]
    canvas.create_text(offset, y, font="Console 10 bold", text=str(toStand[idx]))
    offset = offset + width[2]
    canvas.create_text(offset, y, font="Console 10 bold", text=str(distance[idx]))
    offset = offset + width[3]
    canvas.create_text(offset, y, font="Console 10 bold", text=str(started[idx])[11:19])
    offset = offset + width[4]
    canvas.create_text(offset, y, font="Console 10 bold", text=str(completed[idx])[11:19])

def show_circle(stop, place, canvas):
    radius = 3
    print('stop:{0} long:{1} lat:{2}'.format(stop, get_long(stop), get_lat(stop)))
    x = getX(get_long(stop))
    y = getY(get_lat(stop))
    draw_circle(x, y, radius, canvas)
    canvas.create_text(x, y-9, font="Console 10 bold",text=str(place))
    draw_bearing (x, y, radius, get_bearing(stop), canvas)

def get_long(stop_id):
    for i in range(numbStops-1):
        if stopId[i] == stop_id:
            return longitude[i]

def get_lat(stop_id):
    for i in range(numbStops-1):
        if stopId[i] == stop_id:
            return latitude[i]

def get_bearing(stop_id):
    for i in range(numbStops-1):
        if stopId[i] == stop_id:
            return bearing[i]

try:
    conn = psycopg2.connect("dbname=kabina user=kabina password=kaboot")
    cur = conn.cursor()
    # reading legs
    cur.execute("select route_id, place, from_stand, to_stand, distance, started, completed FROM leg ORDER BY route_id, place")
    rows = cur.fetchall()
    previous = -1
    idx = 0
    
    for row in rows:
        if previous != row[0]: # not the same route
            numbRoutes += 1 
        id[idx]       = numbRoutes
        routeId[idx]  = row[0]
        place[idx]    = row[1]
        fromStand[idx]= row[2]
        toStand[idx]  = row[3]
        distance[idx] = row[4]
        started[idx]  = row[5]
        completed[idx]= row[6]
        previous = row[0]
        idx += 1
    
    numbLegs = idx
    
    # reading stops
    cur.execute("select id, bearing, latitude, longitude, name FROM stop")
    rows = cur.fetchall()
    numbStops = 0
    for row in rows:
        stopId[numbStops]    = row[0]
        bearing[numbStops]   = row[1]
        latitude[numbStops]  = row[2]
        longitude[numbStops] = row[3]
        name[numbStops]      = row[4]
        numbStops += 1
    
    root.bind('<KeyPress>', onKeyPress)
    showRoute(1)
  
    canvas.pack()
    #gif1 = PhotoImage(file='small_globe.gif')
    #canvas.create_image(50, 10, image=gif1, anchor=NW)
    root.mainloop() 
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()

