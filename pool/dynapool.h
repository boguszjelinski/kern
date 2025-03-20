#define NUMBTHREAD 12 // one more than possibly configured
#define MAXINPOOL 4
#define MAXORDID MAXINPOOL*2
#define MAXNODE MAXINPOOL+MAXINPOOL-1
#define MAXTHREADMEM 10000000
#define MAXNODEMEM MAXTHREADMEM * NUMBTHREAD

#define MAXANGLE 120.0
#define MAXANGLEDIST 1
#define STOP_WAIT 1 // minute, how long it takes at a bus stop

#define true 1
#define false 0
typedef int boolean;

void initMem();
void *allocMem(void *);
void *deallocMem(void *);
void freeMem();
void findPool(int, int);

struct Stop {
    long id;  // int
    int bearing; // short
    double longitude;
    double latitude;
};
typedef struct Stop Stop;

struct Order {
    long id;
    int fromStand;
    int toStand;
    int maxWait;
    int maxLoss;
    int distance;
};
typedef struct Order Order;

struct Cab {
    long id; // int
    int location; // short
    int seats;
};
typedef struct Cab Cab;

enum FileType {
    STOPS, ORDERS, CABS, CONFIG
};

struct Branch {
  short cost;
  unsigned char outs; // BYTE, number of OUT nodes, so that we can guarantee enough IN nodes
  short ordNumb; // it is in fact ord number *2; length of vectors below - INs & OUTs
  // ordIDs does not need to be int, short is enough as we NEVER are going to solve pools with 64k orders as input
  short ordIDs[MAXORDID];
  char ordActions[MAXORDID];
  short cab;
};

typedef struct Branch Branch;

void dynapool(int, int[MAXINPOOL - 1],
    short *, int,
    Stop *, int,
    Order *, int, 
    Cab *, int, 
    Branch *, int, 
    int *,
    int [MAXINPOOL - 1]);
