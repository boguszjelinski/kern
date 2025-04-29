#include<stdio.h>
#include<stdlib.h>
#include<math.h>
#include"dynapool.h"

#define MAXSTOPS 5000
#define MAXORDERS 4000
#define MAXCABS 20000
#define MAXRETURN 1000

Stop stops[MAXSTOPS];
Order demand[MAXORDERS];
Cab supply[MAXCABS];
Branch ret[MAXRETURN];
short *dist;

void get_stops(float step, int size) {
    int c = 0;
    for (int i=0; i<size; i++)
        for (int j=0; j<size; j++) {
            stops[c].id = c;
            stops[c].bearing = 0;
            stops[c].latitude = 49.0 + step * i;
            stops[c].longitude = 19.000 + step * j;
            c++;
        }
}

void set_dist(int row, int col, short val) {
    *(dist + (row * MAXSTOPS) + col) = val;
}

short get_dist(int row, int col) {
    return *(dist + (row * MAXSTOPS) + col);
}

#define M_PI 3.14159265358979323846264338327950288
const double M_PI_180 = M_PI / 180.0;
const double REV_M_PI_180 = 180.0 / M_PI;

double deg2rad(double deg) { return deg * M_PI_180; }
double rad2deg(double rad) { return rad * REV_M_PI_180; }

#define CAB_SPEED 30.0

// https://dzone.com/articles/distance-calculation-using-3
double count_dist(double lat1, double lon1, double lat2, double lon2) {
    double theta = lon1 - lon2;
    double dst = sin(deg2rad(lat1)) * sin(deg2rad(lat2)) + cos(deg2rad(lat1))
                  * cos(deg2rad(lat2)) * cos(deg2rad(theta));
    dst = acos(dst);
    dst = rad2deg(dst);
    dst = dst * 60.0 * 1.1515;
    dst = dst * 1.609344;
    return dst;
}

void init_distance(int size) {
    dist = malloc(MAXSTOPS * MAXSTOPS * sizeof(short));
    for (int i=0; i<size; i++) {
        set_dist(i, i, 0);
        for (int j= i+1; j<size; j++) {
            float d = count_dist(stops[i].latitude, stops[i].longitude, stops[j].latitude, stops[j].longitude)
                         * (60.0 / CAB_SPEED);
            if (((int) d) == 0) { d = 1.0; } // a transfer takes at least one minute. 
            set_dist(stops[i].id, stops[j].id, (short) d); // TASK: we might need a better precision - meters/seconds
            set_dist(stops[j].id, stops[i].id, get_dist(stops[i].id, stops[j].id));
        }
    }
}

void get_orders(int size, int stops)  {
    for (int i=0; i<size; i++) {     
        int from = i % stops;
        int to = from + 5 >= stops ? from - 5 : from + 5;
        short dst = get_dist(from, to);
        demand[i].id = i;
        demand[i].fromStand = from;
        demand[i].toStand = to;
        demand[i].maxWait = 15;
        demand[i].maxLoss = 70;
        demand[i].distance = dst;
    }
}

void get_cabs(int size) {
    for (int i=0; i<size; i++) {
        supply[i].id = i;
        supply[i].location =  i % 2400;
        supply[i].seats = 10;
        supply[i].dist = 0;
    }
}

int main() {
    initMem();
    
    int stops_numb = 49;
    int distSize = MAXSTOPS;
    int stopsSize = MAXSTOPS;
    int ordersSize = 60;
    int cabsSize = 1000;
    int retSize = MAXRETURN;

    int numbThreads = 12;
    int poolsize[MAXINPOOL - 1];
    poolsize[0] = 150;
    poolsize[1] = 500;
    poolsize[2] = 1300;

    get_stops(0.03, stops_numb);

    init_distance(stops_numb);    

    get_orders(ordersSize, stops_numb);

    get_cabs(cabsSize);
    
    int count;
    int pooltime[MAXINPOOL - 1];
   
    
    dynapool(numbThreads, poolsize, 
            dist, distSize, 
            stops, stopsSize,
            demand, ordersSize, 
            supply, cabsSize, 
            120, // max angle
            1, // max angle dist
            1, // stop_wait
            0, // unused
            ret, retSize, 
            &count,
            pooltime);
    printf("Pool count: %d\n", count);
    
    freeMem();

    short *supply_out = malloc(10000 * sizeof(short));
    short *demand_out = malloc(10000 * sizeof(short));
    int count2;

    stops_numb = 4999;
    ordersSize = 3000;
    cabsSize = 19000;
    get_orders(ordersSize, stops_numb);
    get_cabs(cabsSize);
    fast_lcm(
        dist,
        distSize,
        demand,
        ordersSize,
        supply,
        cabsSize,
        1000,
        supply_out, // returned values
        demand_out,
        &count2
    );
    printf("LCM output count: %d\n", count2);
}
