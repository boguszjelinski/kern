#define NUMBTHREAD 17
#define MAXINPOOL 5
#define MAXORDID MAXINPOOL*2
#define MAXNODE MAXINPOOL+MAXINPOOL-1
#define MAXTHREADMEM 15000000

#define MAXANGLE 120.0
#define MAXANGLEDIST 1
#define STOP_WAIT 1 // minute, how long it takes at a bus stop

#define true 1
#define false 0
typedef int boolean;

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
  int ordIDs[MAXORDID]; // we could get rid of it to gain on memory (key stores this too); but we would lose time on parsing
  char ordActions[MAXORDID];
  int cab;
};

typedef struct Branch Branch;