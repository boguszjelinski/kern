#define NUMBTHREAD 12 // one more than possibly configured
#define MAXINPOOL 4
#define MAXORDID MAXINPOOL*2
#define MAXNODE MAXINPOOL+MAXINPOOL-1
#ifdef __linux__
    #define MAXTHREADMEM 5000000 // Decreased value due to static memory limit
#elif _WIN32
    #define MAXTHREADMEM 5000000 // You may edit the value for your OS
#else
    #define MAXTHREADMEM 10000000
#endif
#define MAXNODEMEM MAXTHREADMEM * NUMBTHREAD

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
    short dist; // time left to completion of last leg in a route
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
  int cab;
  unsigned char parity; // by how many do OUTs exceed INs, to easily find empty legs 
};

typedef struct Branch Branch;

void dynapool(int, int[MAXINPOOL - 1],
    short *, int,
    Stop *, int,
    Order *, int, 
    Cab *, int, 
    short, short, short,
    char,
    Branch *, int, 
    int *,
    int [MAXINPOOL - 1]);

void fast_lcm(short *dista, int distSize,
        Order *orders_cpy, int ordersSize, 
        Cab *cabs_cpy, int cabsSize, 
        int how_many,
        // returned values, the size is determined by how_many
        int *supply,
        int *demand,
        int *count);
        