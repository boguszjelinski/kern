/// Kabina minibus/taxi dispatcher
/// Copyright (c) 2024 by Bogusz Jelinski bogusz.jelinski@gmail.com
/// 
/// Pool finder submodule.
/// A pool is a group of orders to be picked up by a cab in a prescribed sequence
/// 'Branch' structure describes one such group (saved as route in the database)
/// 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include "dynapool.h"

// MAC: cc -c -Wno-implicit-function-declaration poold.c dynapool.c -w -O3
// ar -cvq libdynapool.a poold.o dynapool.o
// sudo cp libdynapool.a /Library/Developer/CommandLineTools/SDKs/MacOSX11.1.sdk/usr/lib/
// SHARED:
// gcc -Wno-implicit-function-declaration -shared -o libdynapool.so -fPIC -w poold.c dynapool.c

time_t rawtime;
struct tm * timeinfo;

short *distance;
int distNumb;

Stop *stops;
int stopsNumb;

Order *demand;
int demandNumb;

Cab *supply;
int cabsNumb;

extern struct Branch;
typedef struct Branch Branch;

int nodeSize;
int nodeSizeSMP[NUMBTHREAD];

Branch *retNode;
int retCount = 0, retNumb=0;
short maxangle, maxangledist, stop_wait;

extern struct arg_struct {
   int i;
   float chunk;
   int lev;
   int inPool;
} *args[NUMBTHREAD];

// these two called by Rust
void initMem() {
  pthread_t myThread[NUMBTHREAD];
  printf("Init mem start\n");
  for (int i = 0; i<NUMBTHREAD; i++)
    args[i] = malloc(sizeof(struct arg_struct) * 1);
    
  for (int i = 0; i < NUMBTHREAD; i++) { // TASK: allocated orders might be spread unevenly -> count non-allocated and devide chunks ... evenly
    args[i]->i = i; 
    if (pthread_create(&myThread[i], NULL, &allocMem, args[i]) != 0) {
        printf("Err creating thread %d!\n", i);
    }
  }
  for (int i = 0; i<NUMBTHREAD; i++) 
    pthread_join(myThread[i], NULL);
  printf("Init mem stop\n");
}

void freeMem() {
  pthread_t myThread[NUMBTHREAD];
  for (int i = 0; i < NUMBTHREAD; i++) { // TASK: allocated orders might be spread unevenly -> count non-allocated and devide chunks ... evenly
    args[i]->i = i; 
    if (pthread_create(&myThread[i], NULL, &deallocMem, args[i]) != 0) {
        printf("Err creating thread %d!\n", i);
    }
  }
  for (int i = 0; i<NUMBTHREAD; i++) 
    pthread_join(myThread[i], NULL);
  for (int i=0; i<NUMBTHREAD; i++) {
    nodeSizeSMP[i] = 0;
    free(args[i]);
  }
}

extern short dist(int row, int col);

void dynapool(int numbThreads, int poolsize[MAXINPOOL - 1],
              short *dista, int distSize,
              Stop *stands, int stopsSize,
              Order *orders, int ordersSize, 
              Cab *cabs, int cabsSize, 
              short _maxangle, 
              short _maxangledist,
              short _stop_wait,
              char goal_func, // for future use, choice of function goal
              Branch *ret, int retSize, 
              int *count,
              int pooltime[MAXINPOOL - 1]) {
    distNumb = distSize;
    stopsNumb = stopsSize;
    demandNumb = ordersSize;
    cabsNumb = cabsSize;
    retNode = ret; // here we will save the outcome
    retNumb = retSize;

    distance = dista;
    demand = orders;
    supply = cabs;
    stops = stands;
    retNode = ret;

    maxangle = _maxangle;
    maxangledist = _maxangledist;
    stop_wait = _stop_wait;

    retCount = 0; // surprise - static variables keep value between calls, like a daemon
    struct timeval begin, end;
    printf("stops=%d orders=%d cabs=%d\n", stopsSize, ordersSize, cabsSize);
    for (int i=0; i<MAXINPOOL - 1; i++)
      if (demandNumb < poolsize[i]) {
        gettimeofday(&begin, 0);
        findPool(MAXINPOOL - i, numbThreads); 
        gettimeofday(&end, 0);
        long seconds = end.tv_sec - begin.tv_sec;
        long microseconds = end.tv_usec - begin.tv_usec;
        double elapsed = seconds + microseconds*1e-6;
        printf("Pool with %d took %f seconds\n\n", MAXINPOOL - i, elapsed);
        pooltime[i] = elapsed;
      }
    *count = retCount;
}

short dis(short *dista, int dist_size, int row, int col) {
  return *(dista + (row * dist_size) + col);
}

// fast but dummy
// just find the nearest value in the row, not in the whole matrix
void lcm_dummy(short *dista, int distSize,
          Order *orders_cpy, int ordersSize, 
          Cab *cabs_cpy, int cabsSize, 
          int how_many,
          // returned values, the size is determined by how_many
          int *supply,
          int *demand,
          int *count) {
  int big_cost = 1000000;
  int lcm_min_val;
  int smin, dmin, cost;
  Cab *cab = cabs_cpy + 1;
  Order *ord;
  int i; // returned count
  int size = ordersSize > cabsSize ? cabsSize : ordersSize; // MIN
  *count = how_many <size ? how_many : size;

  for (i = 0; i < *count; i++) { // we need to repeat the search (cut off rows/columns) 'howMany' times
    lcm_min_val = big_cost;
    smin = 0;
    // now find the minimal element in the row
    int found = 0; // false
    for (int s = 0; s < cabsSize; s++) {
      cab = cabs_cpy + s;
      if ((*cab).id == -1) {
          continue;
      }
      cost = dis(dista, distSize, (*cab).location, (*ord).fromStand);
      if (cost == 0) { // you can't have a better solution
        smin = s;
        break;
      }
      if (cost < lcm_min_val) { // TODO: we could check wait_time here
        lcm_min_val = cost;
        smin = s;
      }
    }
    // binding cab to the customer order
    *(supply + i) = smin;
    *(demand + i) = i;
    // marking cab as allocated
    (*(cabs_cpy + smin)).id = -1;
  }
}

void slow_lcm(short *dista, int distSize,
          Order *orders_cpy, int ordersSize, 
          Cab *cabs_cpy, int cabsSize, 
          int how_many,
          // returned values, the size is determined by how_many
          int *supply,
          int *demand,
          int *count) {
  int big_cost = 1000000;
  int lcm_min_val;
  int smin;
  int dmin;
  Cab *cab = cabs_cpy + 1;
  Order *ord;
  int cnt = 0; // returned count
  short dst;
  for (int i = 0; i < ordersSize; i++) { // we will iterate MIN(ordersSize, how_many) times
    lcm_min_val = big_cost;
    smin = 0;
    dmin = 0;
    // now find the minimal element in the whole matrix
    int found = 0; // flase
    for (int s = 0; s < cabsSize; s++) {
      cab = cabs_cpy + s;
      if ((*cab).id == -1) continue;
      
      for (int d = 0; d < ordersSize; d++) {
        ord = orders_cpy + d;
        dst = dis(dista, distSize, (*cab).location, (*ord).fromStand) + (*cab).dist; // cab.dist: last leg
        if ((*ord).id != -1 && dst < lcm_min_val) {
          lcm_min_val = dst;
          smin = s;
          dmin = d;
          if (lcm_min_val == 0) { // you can't have a better solution
            found = 1; // true
            break;
          }
        }
      }
      if (found) break;
    }
    if (lcm_min_val == big_cost) {
      // LCM minimal cost is big_cost - no more interesting stuff here
      break;
    }
    // binding cab to the customer order
    ord = orders_cpy + dmin;

    if ((*ord).maxWait >= lcm_min_val) {
      //printf("SOL: %d (%d, %d), ord_id=%d, cab_id=%d, order_from=%d, cab_location=%d, dist=%d\n", 
      //        cnt, smin, dmin, (*ord).id, (*(cabs_cpy + smin)).id, (*ord).fromStand, (*(cabs_cpy + smin)).location, lcm_min_val);
      *(supply + cnt) = smin;
      *(demand + cnt) = dmin;
      // removing the "columns" and "rows" from a virtual matrix
      (*(cabs_cpy + smin)).id = -1;
      (*(orders_cpy + dmin)).id = -1;
      cnt++;
    } else  // only forget that order, you will not find a lower value in the matrix
        (*(orders_cpy + dmin)).id = -1;
    if (cnt >= how_many) { 
      break;
    }
  }
  *count = cnt;
}
