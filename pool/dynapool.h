#define NUMBTHREAD 9
#define MAXINPOOL 4
#define MAXORDID MAXINPOOL*2
#define MAXANGLE 120
#define MAXNODE MAXINPOOL+MAXINPOOL-1
#define MAXTHREADMEM 2500000

#define true 1
#define false 0
typedef int boolean;

void findPool(int, int);

struct Stop {
    int id;
    short bearing;
    double longitude;
    double latitude;
};
typedef struct Stop Stop;

struct Order {
    int id;
    short fromStand;
    short toStand;
    short maxWait;
    short maxLoss;
    short distance;
};
typedef struct Order Order;

struct Cab {
    int id;
    short location;
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
  int ordIDsSorted[MAXORDID]; 
  char ordActionsSorted[MAXORDID];
  int cab;
};

typedef struct Branch Branch;