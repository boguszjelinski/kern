#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "dynapool.h"

Branch nodeSMP[NUMBTHREAD][MAXTHREADMEM];
pthread_t myThread[NUMBTHREAD];

struct arg_struct {
   int i;
   float chunk;
   int lev;
   int inPool;
} *args[NUMBTHREAD];

extern int memSize[MAXNODE];
extern Branch *node[MAXNODE];
extern int nodeSize[MAXNODE];
extern int nodeSizeSMP[NUMBTHREAD];

extern short *distance;
extern int distNumb;

extern Stop *stops;
extern int stopsNumb;

extern Order *demand;
extern int demandNumb;

extern Cab *supply;
extern int cabsNumb;

extern Branch *retNode;
extern int retCount, retNumb; // number of branches returned to Rust

short dist(int row, int col) {
  return *(distance + (row * distNumb) + col);
}

void storeBranch(int thread, char action, int lev, int ordId, Branch *b, int inPool) {
    if (nodeSizeSMP[thread] >= MAXTHREADMEM) {
      printf("storeBranch: allocated mem too low, level: %d, inPool: %d\n", lev, inPool);
      return;
    }
    Branch *ptr = &nodeSMP[thread][nodeSizeSMP[thread]];
    ptr->ordNumb = inPool + inPool - lev;
    ptr->ordIDs[0] = ordId;
    ptr->ordActions[0] = action;
    ptr->ordIDsSorted[0] = ordId;
    ptr->ordActionsSorted[0] = action;
    // ? memcpy
    for (int j = 0; j < ptr->ordNumb - 1; j++) { // further stage has one passenger less: -1
      ptr->ordIDs[j + 1]      = b->ordIDs[j];
      ptr->ordActions[j + 1]  = b->ordActions[j];
      ptr->ordIDsSorted[j + 1]= b->ordIDs[j];
      ptr->ordActionsSorted[j + 1] = b->ordActions[j];
    }
    //sprintf (ptr->key, "%d%c%s", ordId, action, b->key);
    ptr->cost = dist (action == 'i' ? demand[ordId].fromStand : demand[ordId].toStand,
                      b->ordActions[0] == 'i' ? demand[b->ordIDs[0]].fromStand : demand[b->ordIDs[0]].toStand) + b->cost;
    ptr->outs = action == 'o' ? b->outs + 1: b->outs;
    nodeSizeSMP[thread]++;
}

// branch is index of existing Branch in lev+1
void storeBranchIfNotFoundDeeperAndNotTooLong(int thread, int lev, int ordId, int branch, int inPool) {
    // two situations: c IN and c OUT
    // c IN has to have c OUT in level+1, and c IN cannot exist in level + 1
    // c OUT cannot have c OUT in level +1
    boolean inFound = false;
    boolean outFound = false;
    Branch *ptr = &node[lev+1][branch];
    for (int i = 0; i < ptr->ordNumb; i++) {
      if (ptr->ordIDs[i] == ordId) {
        if (ptr->ordActions[i] == 'i') {
          inFound = true;
        } else {
          outFound = true;
        }
        // current passenger is in the branch below
      }
    }
    // now checking if anyone in the branch does not lose too much with the pool
    // c IN
    int nextStop = ptr->ordActions[0] == 'i'
                    ? demand[ptr->ordIDs[0]].fromStand : demand[ptr->ordIDs[0]].toStand;
    if (!inFound
        && outFound
        && !isTooLong(dist(demand[ordId].fromStand, nextStop), ptr)
        // TASK? if the next stop is OUT of passenger 'c' - we might allow bigger angle
        && bearingDiff(stops[demand[ordId].fromStand].bearing, stops[nextStop].bearing) < MAXANGLE
        ) storeBranch(thread, 'i', lev, ordId, ptr, inPool);
    // c OUT
    if (lev > 0 // the first stop cannot be OUT
        && ptr->outs < inPool // numb OUT must be numb IN
        && !outFound // there is no such OUT later on
        && !isTooLong(dist(demand[ordId].toStand, nextStop), ptr)
        && bearingDiff(stops[demand[ordId].toStand].bearing, stops[nextStop].bearing) < MAXANGLE
        ) storeBranch(thread, 'o', lev, ordId, ptr, inPool);
}

void iterate(void *arguments) {
  struct arg_struct *ar = arguments;
  int size = round((ar->i + 1) * ar->chunk) > demandNumb ? demandNumb : round((ar->i + 1) * ar->chunk);
  
  for (int ordId = round(ar->i * ar->chunk); ordId < size; ordId++) 
   if (demand[ordId].id != -1) { // not allocated in previous search (inPool+1)
    for (int b = 0; b < nodeSize[ar->lev + 1]; b++) 
      if (node[ar->lev + 1][b].cost != -1) {  
        // we iterate over product of the stage further in the tree: +1
        storeBranchIfNotFoundDeeperAndNotTooLong(ar->i, ar->lev, ordId, b, ar->inPool);
    }
  }
}

void addBranch(int id1, int id2, char dir1, char dir2, int outs, int lev)
{
    if (nodeSize[lev] >= memSize[lev]) {
      printf("addBranch: allocated mem too low, level: %d\n", lev);
      return;
    }
    Branch *ptr = &node[lev][nodeSize[lev]];

    if (id1 < id2 || (id1==id2 && dir1 == 'i')) {
        ptr->ordIDsSorted[0] = id1;
        ptr->ordIDsSorted[1] = id2;
        ptr->ordActionsSorted[0] = dir1;
        ptr->ordActionsSorted[1] = dir2;
    }
    else if (id1 > id2 || id1 == id2) {
        ptr->ordIDsSorted[0] = id2;
        ptr->ordIDsSorted[1] = id1;
        ptr->ordActionsSorted[0] = dir2;
        ptr->ordActionsSorted[1] = dir1;
    }
    ptr->cost = dist(demand[id1].toStand, demand[id2].toStand);
    ptr->outs = outs;
    ptr->ordIDs[0] = id1;
    ptr->ordIDs[1] = id2;
    ptr->ordActions[0] = dir1;
    ptr->ordActions[1] = dir2;
    ptr->ordNumb = 2;
    nodeSize[lev]++;
}

void storeLeaves(int lev) {
    for (int c = 0; c < demandNumb; c++)
      if (demand[c].id != -1) // assigned in inPool=4 while looking for inPool=3
        for (int d = 0; d < demandNumb; d++)
          if (demand[d].id != -1) {
            // to situations: <1in, 1out>, <1out, 2out>
            if (c == d)  {
                // IN and OUT of the same passenger, we don't check bearing as they are probably distant stops
                addBranch(c, d, 'i', 'o', 1, lev);
            } else if (dist(demand[c].toStand, demand[d].toStand)
                        < dist(demand[d].fromStand, demand[d].toStand) * (100.0 + demand[d].maxLoss) / 100.0
                    && bearingDiff(stops[demand[c].toStand].bearing, stops[demand[d].toStand].bearing) < MAXANGLE
            ) {
              // TASK - this calculation above should be replaced by a redundant value in taxi_order - distance * loss
              addBranch(c, d, 'o', 'o', 2, lev);
              /*printf("c=%d d=%d c.id=%d d.id=%d c.to=%d d.from=%d d.to=%d d.loss=%d c.to.bearing=%d d.to.bearing=%d dist_c_d=%d dist_d_d=%d\n",
                  c, d, demand[c].id, demand[d].id, demand[c].toStand, demand[d].fromStand, demand[d].toStand,
                  demand[d].maxLoss, stops[demand[c].toStand].bearing, stops[demand[d].toStand].bearing,
                  dist(demand[c].toStand, demand[d].toStand), dist(demand[d].fromStand, demand[d].toStand));
                  */
            }
          }
}

void dive(int lev, int inPool, int numbThreads)
{
    //printf("DIVE, inPool: %d, lev:%d\n", inPool, lev);
    if (lev > inPool + inPool - 3) { // lev >= 2*inPool-2, where -2 are last two levels
      storeLeaves(lev);
      return; // last two levels are "leaves"
    }
    dive(lev + 1, inPool, numbThreads);
    const float chunk = demandNumb / numbThreads;
    if (round(numbThreads*chunk) < demandNumb) numbThreads++; // last thread will be the reminder of division

    for (int i = 0; i<numbThreads; i++) { // TASK: allocated orders might be spread unevenly -> count non-allocated and devide chunks ... evenly
        args[i]->i = i; 
        args[i]->chunk = chunk; 
        args[i]->lev = lev; 
        args[i]->inPool = inPool;
        nodeSizeSMP[i] = 0;
        if (pthread_create(&myThread[i], NULL, &iterate, args[i]) != 0) {
            printf("Err creating thread %d!\n", i);
        }
    }
    for (int i = 0; i<numbThreads; i++) {
        pthread_join(myThread[i], NULL); // Wait until thread is finished 
    }
    int idx = 0;
    for (int i = 0; i<numbThreads; i++) {
        if (idx + nodeSizeSMP[i] >= memSize[lev]) {
          printf("dive: allocated mem too low, level: %d\n", lev);
          break;
        }
        memcpy(&node[lev][idx], nodeSMP[i], nodeSizeSMP[i] * sizeof(Branch));
        idx += nodeSizeSMP[i];
    }
    nodeSize[lev] = idx;
}

int bearingDiff(int a, int b) 
{
    int r = (a - b) % 360;
    if (r < -180.0) {
      r += 360.0;
    } else if (r >= 180.0) {
      r -= 360.0;
    }
    return abs(r);
}

boolean isTooLong(int wait, Branch *b)
{
    for (int i = 0; i < b->ordNumb; i++) {
        if (wait >  //distance[demand[b->ordIDs[i]].fromStand][demand[b->ordIDs[i]].toStand] 
                demand[b->ordIDs[i]].distance * (100.0 + demand[b->ordIDs[i]].maxLoss) / 100.0) 
                  return true;
        if (b->ordActions[i] == 'i' && wait > demand[b->ordIDs[i]].maxWait) return true;
        if (i + 1 < b->ordNumb) 
            wait += dist(b->ordActions[i] == 'i' ? demand[b->ordIDs[i]].fromStand : demand[b->ordIDs[i]].toStand,
                         b->ordActions[i + 1] == 'i' ? demand[b->ordIDs[i + 1]].fromStand : demand[b->ordIDs[i + 1]].toStand);
    }
    return false;
}

int compareCost(const void * a, const void * b)
{
  Branch *brA = (Branch *)a;
  Branch *brB = (Branch *)b;
  return brA->cost - brB->cost;
}

int countNodeSize(int lev) {
  int count=0;
  Branch *arr = node[lev];
  for (int i=0; i<nodeSize[lev]; i++)
    if (arr[i].cost != -1 ) count++;
  return count;
}

void rmFinalDuplicates(int inPool) {
    int lev = 0;
    int cabIdx = -1;
    int from;
    int distCab;
    int size = nodeSize[lev];
    Branch *arr = node[lev];
    register Branch *ptr;

    if (nodeSize[lev] < 1) return;

    qsort(arr, size, sizeof(Branch), compareCost);
    
    for (int i = 0; i < size; i++) {
      ptr = arr + i;
      if (ptr->cost == -1) continue; // not dropped earlier or (!) later below
      from = demand[ptr->ordIDs[0]].fromStand;
      cabIdx = findNearestCab(from);
      if (cabIdx == -1) { // no more cabs
        // mark th rest of pools as dead
        // TASK: why? we won't use this information, node[0] will be garbage-collected
        printf("NO CAB\n");
        for (int j = i + 1; j < size; j++) arr[j].cost = -1;
        break;
      }
      distCab = dist(supply[cabIdx].location, from);
      if (distCab == 0 // constraints inside pool are checked while "diving" in recursion
              || constraintsMet(ptr, distCab)) {
        // hipi! we have a pool
        ptr->cab = cabIdx; // not supply[cabIdx].id as it is faster to reference it in Boot (than finding IDs)
        // mark cab and order ass allocated
        supply[ptr->cab].id = -1; // allocated
        for (int o=0; o < ptr->ordNumb; o++) // ordNumb is pool*2 but 'if' would cost more
          demand[ptr->ordIDs[o]].id = -1;
        if (retCount < retNumb) {
          *(retNode + retCount++) = *ptr; // TASK: maybe copy of pointers would do ? 
        }
        // remove any further duplicates
        for (int j = i + 1; j < size; j++)
          if (arr[j].cost != -1 && isFound(ptr, arr+j, inPool+inPool-1)) // -1 as last action is always OUT
            arr[j].cost = -1; // duplicated; we remove an element with greater costs (list is pre-sorted)
      } else ptr->cost = -1; // constraints not met, mark as unusable
    } 
}

boolean constraintsMet(Branch *el, int distCab) {
  // TASK: distances in pool should be stored to speed-up this check
  int dst = 0;
  Order *o, *o2;
  for (int i = 0; i < el->ordNumb; i++) {
    o = &demand[el->ordIDs[i]];
    if (el->ordActions[i] == 'i' && dst + distCab > o->maxWait) {
      return false;
    }
    if (el->ordActions[i] == 'o' && dst > (1 + o->maxLoss/100.0) * o->distance) { // TASK: remove this calcul
      return false;
    }
    o2 = &demand[el->ordIDs[i + 1]];
    if (i < el->ordNumb - 1) {
      dst += dist(el->ordActions[i] == 'i' ? o->fromStand : o->toStand,
                  el->ordActions[i + 1] == 'i' ? o2->fromStand : o2->toStand);
    }
  }
  return true;
}

boolean isFound(Branch *br1, Branch *br2, int size) 
{   
    for (int x = 0; x < size; x++)
      if (br1->ordActions[x] == 'i') 
        for (int y = 0; y < size; y++) 
          if (br2->ordActions[y] == 'i' && br2->ordIDs[y] == br1->ordIDs[x])
            return true;
    return false;
}

int findNearestCab(int from) {
    int dst = 10000; // big enough
    int nearest = -1;
    for (int i = 0; i < cabsNumb; i++) {
      if (supply[i].id == -1) // allocated earlier to a pool
        continue;
      if (dist(supply[i].location, from) < dst) {
        dst = dist(supply[i].location, from);
        nearest = i;
      }
    }
    return nearest;
}

void findPool(int inPool, int numbThreads) {
    if (inPool > MAXINPOOL) {
      return;
    }
    for (int i=0; i<MAXNODE; i++) nodeSize[i] = 0;
    for (int i=0; i<NUMBTHREAD; i++) nodeSizeSMP[i] = 0;
    dive(0, inPool, numbThreads);
    for (int i = 0; i < inPool + inPool - 1; i++)
        printf("node[%d].size: %d\n", i, countNodeSize(i));
    rmFinalDuplicates(inPool);
    printf("FINAL: inPool: %d, found pools: %d\n", inPool, countNodeSize(0));
}
