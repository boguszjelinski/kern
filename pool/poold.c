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

//int maxInPool[MAXINPOOL - 1] = {80, 160, 300, 600}; // see main.rs !!! these are overwritten
//int inPool[MAXINPOOL - 1] = {5, 4, 3, 2};

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

int memSize[MAXNODE] = {50000000, 100000000, 100000000, 100000000, 100000000, 100000000, 50000000, 1000000, 50000};
Branch *node[MAXNODE];
int nodeSize[MAXNODE];
int nodeSizeSMP[NUMBTHREAD];

Branch *retNode;
int retCount = 0, retNumb=0;

volatile sig_atomic_t done = 0;

extern struct arg_struct {
   int i;
   float chunk;
   int lev;
   int inPool;
} *args[NUMBTHREAD];

void handle_signal(int signum) {
   done = 1;
}

void initMem() {
  for (int i=0; i<MAXNODE; i++) {
    node[i] = malloc(sizeof(Branch) * memSize[i]);
    if (node[i] == NULL) {
      printf("Error allocating node mem");
      exit(0);
    }
  }
  for (int i = 0; i<NUMBTHREAD; i++)
    args[i] = malloc(sizeof(struct arg_struct) * 1);
}

void freeMem() {
  for (int i=0; i<MAXNODE; i++) {
    free(node[i]);
    nodeSize[i] = 0;
  }
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
    // signal(SIGINT, handle_signal);
    // signal(SIGTERM, handle_signal);
    // signal(SIGABRT, handle_signal);
    printf("Orders: %d\nCabs: %d\n", ordersSize, cabsSize);
    //initMem(); called by Rust
    
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
    //freeMem();
}
