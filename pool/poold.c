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

Branch node[MAXNODEMEM];
int nodeSize;
int nodeSizeSMP[NUMBTHREAD];

Branch *retNode;
int retCount = 0, retNumb=0;

extern struct arg_struct {
   int i;
   float chunk;
   int lev;
   int inPool;
} *args[NUMBTHREAD];

// these two called by Rust
void initMem() {
  for (int i = 0; i<NUMBTHREAD; i++)
    args[i] = malloc(sizeof(struct arg_struct) * 1);
}

void freeMem() {
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
              Branch *ret, int retSize, 
              int *count,
              int pooltime[MAXINPOOL - 1]) {
    printf("Orders: %d\nCabs: %d\n", ordersSize, cabsSize);
    
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

    retCount = 0; // surprise - static variables keep value between calls, like a daemon
    struct timeval begin, end;

    for (int i=0; i<MAXINPOOL - 1; i++)
      if (demandNumb < poolsize[i]) {
        gettimeofday(&begin, 0);
        findPool(MAXINPOOL - i, numbThreads); 
        gettimeofday(&end, 0);
        long seconds = end.tv_sec - begin.tv_sec;
        long microseconds = end.tv_usec - begin.tv_usec;
        double elapsed = seconds + microseconds*1e-6;
        printf("Pool with %d took %f seconds\n", MAXINPOOL - i, elapsed);
        pooltime[i] = elapsed;
      }
    
    *count = retCount;
}

inline short dis(short *dista, int dist_size, int row, int col) {
  return *(dista + (row * dist_size) + col);
}

void c_lcm(short *dista, int distSize,
          Order *orders_cpy, int ordersSize, 
          Cab *cabs_cpy, int cabsSize, 
          int how_many,
          // returned values, the size is determined by how_many
          short *supply,
          short *demand,
          int *count) {
  int big_cost = 1000000;
  int lcm_min_val;
  int smin;
  int dmin;
  Cab *cab = cabs_cpy + 1;
  Order *ord;
  int i; // returned count
        
  for (i = 0; i < how_many; i++) { // we need to repeat the search (cut off rows/columns) 'howMany' times
    lcm_min_val = big_cost;
    smin = 0;
    dmin = 0;
    // now find the minimal element in the whole matrix
    int found = 0; // flase
    for (int s = 0; s < cabsSize; s++) {
      cab = cabs_cpy + s;
      if ((*cab).id == -1) {
          continue;
      }
      for (int d = 0; d < ordersSize; d++) {
        ord = orders_cpy + d;
        if ((*ord).id != -1 && dis(dista, distSize, (*cab).location, (*ord).fromStand) < lcm_min_val) {
          lcm_min_val = dis(dista, distSize, (*cab).location, (*ord).fromStand);
          smin = s;
          dmin = d;
          if (lcm_min_val == 0) { // you can't have a better solution
            found = 1; // true
            break;
          }
        }
      }
      if (found) {
        break;
      }
    }
    if (lcm_min_val == big_cost) {
      // LCM minimal cost is big_cost - no more interesting stuff here
      break;
    }
    // binding cab to the customer order
    *(supply + i) = smin;
    *(demand + i) = dmin;
    // removing the "columns" and "rows" from a virtual matrix
    (*(cabs_cpy + smin)).id = -1;
    (*(orders_cpy + dmin)).id = -1;
  }
  *count = i;
}
