/// Kabina minibus/taxi dispatcher
/// Copyright (c) 2024 by Bogusz Jelinski bogusz.jelinski@gmail.com
/// 
/// Pool finder submodule.
/// A pool is a group of orders to be picked up by a cab in a prescribed sequence
/// 'Branch' structure describes one such group (saved as route in the database)
/// 

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "dynapool.h"

// each thread has its own chunk of branches, they will be merged
Branch nodeSMP[NUMBTHREAD][MAXTHREADMEM];
pthread_t myThread[NUMBTHREAD];

// thread arguments - which part of a task each gets
struct arg_struct {
   int i;
   int chunk;
   int lev;
   int inPool;
} *args[NUMBTHREAD];

extern int memSize[MAXNODE]; // sizes of statically allocated memory
extern Branch *node[MAXNODE];  // main memory consumer 
extern int nodeSize[MAXNODE]; // actual size of branches
extern int nodeSizeSMP[NUMBTHREAD]; // size of thread memory

// pointers allocated and passed by Rust
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

//inline - it causes linker problems while running 'cargo build'
short dist(int row, int col) {
  return *(distance + (row * distNumb) + col);
}

/// adding an order to a pool
///  b is existing Branch in lev+1
/// 
/// adds an extended pool to the current level (temporary SMP memory)
void storeBranch(int thread, char action, int lev, int ordId, Branch *b, int inPool) {
    if (nodeSizeSMP[thread] >= MAXTHREADMEM) {
      printf("storeBranch: allocated mem too low, level: %d, inPool: %d\n", lev, inPool);
      return;
    }
    Branch *ptr = &nodeSMP[thread][nodeSizeSMP[thread]];
    ptr->ordNumb = inPool + inPool - lev;
    ptr->ordIDs[0] = ordId;
    ptr->ordActions[0] = action;
    // ? memcpy
    // make space for the new order - TODO: maybe we could have the last order at [0]? the other way round
    /*for (int j = 0; j < ptr->ordNumb - 1; j++) { // further stage has one passenger less: -1
      ptr->ordIDs[j + 1]      = b->ordIDs[j];
      ptr->ordActions[j + 1]  = b->ordActions[j];
    }*/
    size_t size = (ptr->ordNumb - 1) * sizeof(int);
    memcpy(&ptr->ordIDs[1], &b->ordIDs[0], size);
    memcpy(&ptr->ordActions[1], &b->ordActions[0], size);

    //sprintf (ptr->key, "%d%c%s", ordId, action, b->key);
    short from = action == 'i' ? demand[ordId].fromStand : demand[ordId].toStand;
    short to = b->ordActions[0] == 'i' ? demand[b->ordIDs[0]].fromStand : demand[b->ordIDs[0]].toStand;
    ptr->cost = dist(from, to) + (b->cost + from == to ? 0 : STOP_WAIT);
    ptr->outs = action == 'o' ? b->outs + 1: b->outs;
    nodeSizeSMP[thread]++;
}

// we need to check the following distances while iterating thru IDs
// 1) if the new branch is 'i' then wait until 'o' with this ID and check max_loss against sum of distances 
//    between ID-s ("if there is movement" takes more time than taking ZERO from DIST)
//    We only need to check max_loss once when we put 'i' 
// 2) while iterating if you encounter any 'i' check wait of that ID
boolean isTooLong(int ordId, char oper, int start_wait, Branch *b) {
  int from, to, wait;
  wait = start_wait;
  for (int i = 0; i < b->ordNumb - 1; i++) {
      if (b->ordActions[i] == 'o' && oper == 'i' && ordId == b->ordIDs[i] &&
          wait >  //distance[demand[b->ordIDs[i]].fromStand][demand[b->ordIDs[i]].toStand] 
              demand[ordId].distance * (100.0 + demand[ordId].maxLoss) / 100.0) // this value could be stored, do not calculate each time
                // max loss of the new order (which we are trying to put in) is violated
                // max loss check of other orders have been checked earlier, here, in lev+1, of course only that with IN & OUT
                return true;
      if (b->ordActions[i] == 'i' && wait > demand[b->ordIDs[i]].maxWait) 
        // wait time of an already existing order (in the pool; lev+1) is violated
        return true;
      from = b->ordActions[i] == 'i' ? demand[b->ordIDs[i]].fromStand : demand[b->ordIDs[i]].toStand;
      to = b->ordActions[i + 1] == 'i' ? demand[b->ordIDs[i + 1]].fromStand : demand[b->ordIDs[i + 1]].toStand;
      if (from != to) wait += dist(from, to) + STOP_WAIT;
  }
  // just check the last 'o', if it is the OUT of the order that we are checking now (with IN)
  if (oper == 'i' && ordId == b->ordIDs[b->ordNumb - 1] &&
      wait > demand[ordId].distance * (100.0 + demand[ordId].maxLoss) / 100.0) // this value could be stored, do not calculate each time
          return true;
  return false;
}
/// check how an order fits into a pool
/// branch is index of existing Branch in lev+1
/// returns: just pushes to temporary array
void storeBranchIfNotFoundDeeperAndNotTooLong(int thread, int lev, int ordId, int branch, int inPool) {
    // two situations: c IN and c OUT
    // c IN has to have c OUT in level+1, and c IN cannot exist in level + 1
    // c OUT cannot have c OUT in level +1
    boolean outFound = false;
    Branch *ptr = &node[lev+1][branch];
    for (int i = 0; i < ptr->ordNumb; i++) {
      if (ptr->ordIDs[i] == ordId) {
        if (ptr->ordActions[i] == 'i') {
          //inFound = true; what was I thinking - if IN is found it must be also OUT, we cannot proceed
          return;
        } else {
          outFound = true;
          break; //
        }
      }
    }
    // now checking if anyone in the branch does not lose too much with the pool
    // c IN
    int nextStop = ptr->ordActions[0] == 'i'
                    ? demand[ptr->ordIDs[0]].fromStand : demand[ptr->ordIDs[0]].toStand;
    if (outFound) {
      if (!isTooLong(ordId, 'i', dist(demand[ordId].fromStand, nextStop) 
                                  + (demand[ordId].fromStand != nextStop ? STOP_WAIT : 0), ptr)
        // TASK? if the next stop is OUT of passenger 'c' - we might allow bigger angle
        && (dist(demand[ordId].fromStand, nextStop) > MAXANGLEDIST 
            || bearingDiff(stops[demand[ordId].fromStand].bearing, stops[nextStop].bearing) < MAXANGLE)
        ) 
        storeBranch(thread, 'i', lev, ordId, ptr, inPool);
    }
    // c OUT
    else if (lev > 0 // the first stop cannot be OUT
        && ptr->outs < inPool // numb OUT must be numb IN
        && !isTooLong(ordId, 'o', dist(demand[ordId].toStand, nextStop)
                                + (demand[ordId].toStand != nextStop ? STOP_WAIT : 0), ptr)
        && (dist(demand[ordId].toStand, nextStop) > MAXANGLEDIST 
           || bearingDiff(stops[demand[ordId].toStand].bearing, stops[nextStop].bearing) < MAXANGLE)
        ) storeBranch(thread, 'o', lev, ordId, ptr, inPool);
}

/// just a loop and calling store_branch...
void iterate(void *arguments) {
  struct arg_struct *ar = arguments;
  int stop = (ar->i + 1) * ar->chunk;
  if (stop > demandNumb) stop = demandNumb;
  
  for (int ordId = ar->i * ar->chunk; ordId < stop; ordId++) 
   if (demand[ordId].id != -1) { // not allocated in previous search (inPool+1)
    for (int b = 0; b < nodeSize[ar->lev + 1]; b++) 
      //if (node[ar->lev + 1][b].cost != -1) {  we do not set the value any longer, an old check of duplicates
        // we iterate over product of the stage further in the tree: +1
        storeBranchIfNotFoundDeeperAndNotTooLong(ar->i, ar->lev, ordId, b, ar->inPool);
    //}
  }
}

void addBranch(int id1, int id2, char dir1, char dir2, int outs, int lev) {
    if (nodeSize[lev] >= memSize[lev]) {
      printf("addBranch: allocated mem too low, level: %d\n", lev);
      return;
    }
    Branch *ptr = &node[lev][nodeSize[lev]];

    ptr->cost = dist(demand[id1].toStand, demand[id2].toStand) 
                + (demand[id1].toStand == demand[id2].toStand ? 0 : STOP_WAIT);
    ptr->outs = outs;
    ptr->ordIDs[0] = id1;
    ptr->ordIDs[1] = id2;
    ptr->ordActions[0] = dir1;
    ptr->ordActions[1] = dir2;
    ptr->ordNumb = 2;
    nodeSize[lev]++;
}

/// generate permutatations of leaves - last two stops (well, it might be one stop), we skip some checks here
/// just two nested loops
/// a leafe is e.g.: 1out-2out or 1in-1out, the last one must be OUT, 'o'
void storeLeaves(int lev) {
    for (int c = 0; c < demandNumb; c++)
      if (demand[c].id != -1) // assigned in inPool=4 while looking for inPool=3
        for (int d = 0; d < demandNumb; d++)
          if (demand[d].id != -1) {
            // to situations: <1in, 1out>, <1out, 2out>
            if (c == d) {
              // 'bearing' checks if stops are in line, it promotes straight paths to avoid unlife solutions
              // !! we might not check bearing here as they are probably distant stops
              if (dist(demand[c].fromStand, demand[d].toStand) > MAXANGLEDIST || bearingDiff(stops[demand[c].fromStand].bearing, stops[demand[d].toStand].bearing) < MAXANGLE)  {
                // IN and OUT of the same passenger
                addBranch(c, d, 'i', 'o', 1, lev);
              }
            } 
            // now <1out, 2out>
            else if (dist(demand[c].toStand, demand[d].toStand)
                        < dist(demand[d].fromStand, demand[d].toStand) * (100.0 + demand[d].maxLoss) / 100.0
                    && (dist(demand[c].toStand, demand[d].toStand) > MAXANGLEDIST || bearingDiff(stops[demand[c].toStand].bearing, stops[demand[d].toStand].bearing) < MAXANGLE)
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

/// finding all feasible pools - sequences of passengers' pick-ups and drop-offs 
/// recursive dive in the permutation tree
/// level ZERO will have (in 'node' variable) all pickups and dropoffs, 
/// node ONE will miss the first IN marked with 'i' in 'ord_actions'
/// twice as much depths as passengers in pool (pickup and dropoff), 
/// minus leaves generated by a dedicated, simple routine  
/// 
/// lev: starting always with zero
/// in_pool: number of passengers going together
void dive(int lev, int inPool, int numbThreads) {
  //printf("DIVE, inPool: %d, lev:%d\n", inPool, lev);
  if (lev > inPool + inPool - 3) { // lev >= 2*inPool-2, where -2 are last two levels
    storeLeaves(lev);
    return; // last two levels are "leaves"
  }
  dive(lev + 1, inPool, numbThreads);
  int chunk = demandNumb / numbThreads;
  if (chunk == 0) chunk = 1;
  if (numbThreads * chunk < demandNumb) numbThreads++; // last thread will be the reminder of division
  // but with small numbers (demand) it still might be not enough
  // all this will run faster then rounding/float variables
  if (numbThreads * chunk < demandNumb) chunk *= 2;
  //printf("thr=%d chunk=%d\n", numbThreads, chunk);
  // run the threads, each thread gets its own range of orders to iterate over - hence 'iterate'
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

  // join the threads
  for (int i = 0; i<numbThreads; i++) {
      pthread_join(myThread[i], NULL); // Wait until thread is finished 
  }

  // collect the data from threads
  // there might be 'duplicates', 1-2-3 and 1-3-2 and so on, they will be filtered out later
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
  /*
  if (lev ==7) 
    for (int i=0; i<nodeSize[lev]; i++) {
      for (int j=0; j<node[lev][i].ordNumb; j++) 
        printf("%d,", node[lev][i].ordIDs[j]);
      printf("\n");
    }
  */
}

int bearingDiff(int a, int b) {
  int r = (a - b) % 360;
  if (r < -180.0) {
    r += 360.0;
  } else if (r >= 180.0) {
    r -= 360.0;
  }
  return abs(r);
}

int compareCost(const void * a, const void * b) {
  Branch *brA = (Branch *)a;
  Branch *brB = (Branch *)b;
  return brA->cost - brB->cost;
}

int compareCostDetour(const void * a, const void * b) {
  Branch *brA = (Branch *)a;
  Branch *brB = (Branch *)b;
  int comp = brA->cost - brB->cost;
  if (comp == 0) {
    return brA->cab - brB->cab; // cab? it temporarily contains distance without passengers
  }
  return comp;
}

int countNodeSize(int lev) {
  int count=0;
  Branch *arr = node[lev];
  for (int i=0; i<nodeSize[lev]; i++)
    if (arr[i].cost != -1 ) count++;
  return count;
}

void showBranch(int no, Branch *ptr) {
  printf("%d: cost=%d, outs=%d, ordNumb=%d, cab=%d,{", no, ptr->cost, ptr->outs, ptr->ordNumb);
  for (int i=0; i < ptr->ordNumb; i++) printf("%d%c,", ptr->ordIDs[i], ptr->ordActions[i]);
  printf("}\n");   
}

int countPassengers(Branch *ptr) {
  int curr_count = 0;
  int max_count = 0;
  for (int i = 0; i < ptr->ordNumb; i++) {
    if (ptr->ordActions[i] == 'i') {
      curr_count++;
      if (curr_count > max_count) max_count = curr_count; // max_count++ would be the same; which one is faster?
    } else curr_count --; // 'o'
  }
  return max_count;
}

/// there might be pools with same passengers (orders) but in different ... order (sequence of INs and OUTs) 
/// the list will be sorted by total length of the pool, worse pools with same passengers will be removed
/// cabs will be assigned with greedy method 
void rmDuplicatesAndFindCab(int inPool) {
    int lev = 0;
    int cabIdx = -1;
    int from;
    int distCab;
    int size = nodeSize[lev];
    Branch *arr = node[lev];
    register Branch *ptr;
    if (nodeSize[lev] < 1) return;

    /* 
    for (int i = 0; i< size; i++) {
      ptr = arr + i;
      ptr -> cost = sumDetour(ptr); // TODO: goal function in config file
      ptr -> cab = countDistanceWithoutPassengers(ptr); // cab? I don't want to change Branch structure right now, which is mapped to a Rust structure
    }
    qsort(arr, size, sizeof(Branch), compareCostDetour);
    */
    qsort(arr, size, sizeof(Branch), compareCost);

    for (int i = 0; i < size; i++) {
      ptr = arr + i;
      if (ptr->cost == -1) continue; // not dropped earlier or (!) later below
      from = demand[ptr->ordIDs[0]].fromStand;
      cabIdx = findNearestCab(from, countPassengers(ptr));
      if (cabIdx == -1) { // no more cabs
        // mark th rest of pools as dead
        // TASK: why? we won't use this information, node[0] will be garbage-collected
        printf("NO CAB\n");
        for (int j = i + 1; j < size; j++) arr[j].cost = -1;
        break;
      } else if (cabIdx == -2) { // there is no cab for so many passengers
        ptr->cost = -1;
        continue;
      }
      distCab = dist(supply[cabIdx].location, from);
      if (distCab == 0 // constraints inside pool are checked while "diving" in recursion
              || constraintsMet(i, ptr, distCab + STOP_WAIT)) { // for the first passenger STOP_WAIT is wrong, but it will concern the others
        // hipi! we have a pool
        ptr->cab = cabIdx; // not supply[cabIdx].id as it is faster to reference it in Boot (than finding IDs)
        // mark cab and order ass allocated
        supply[ptr->cab].id = -1; // allocated

        for (int o=0; o < ptr->ordNumb; o++) // ordNumb is pool*2 but 'if' would cost more
          demand[ptr->ordIDs[o]].id = -1;
        if (retCount < retNumb) {
          //showBranch(retCount, ptr);
          *(retNode + retCount++) = *ptr; // TASK: maybe copy of pointers would do ? 
        }
        // remove any further duplicates
        for (int j = i + 1; j < size; j++)
          if (arr[j].cost != -1 && isFound(ptr, arr+j, inPool+inPool-1)) // -1 as last action is always OUT
            arr[j].cost = -1; // duplicated; we remove an element with greater costs (list is pre-sorted)      
      } else ptr->cost = -1; // constraints not met, mark as unusable
    } 
}

/// checking max wait of all orders
// maxWait check only, maxLoss is checked in isTooLong
boolean constraintsMet(int idx, Branch *el, int distCab) {
  // TASK: distances in pool should be stored to speed-up this check
  int dst = distCab;
  Order *o, *o2;
  int from, to;
  for (int i = 0; i < el->ordNumb -1; i++) {
    o = &demand[el->ordIDs[i]];
    if (el->ordActions[i] == 'i' && dst > o->maxWait) 
      return false;
    o2 = &demand[el->ordIDs[i + 1]];
    from = el->ordActions[i] == 'i' ? o->fromStand : o->toStand;
    to = el->ordActions[i + 1] == 'i' ? o2->fromStand : o2->toStand;
    if (from != to) dst += dist(from, to) + STOP_WAIT;
  }
  // we don't need to check the last leg as it does not concern "loss", this has been check earlier 
  return true;
}

// needed to sort the result by detour
int sumDetour(Branch *el) {
  Order *o, *o2;
  int from, to, dst, sum = 0;
  for (int i = 0; i < el->ordNumb - 1; i++) {
    if (el->ordActions[i] == 'i') { // now find 'o' and count detour
      dst = 0;
      for (int j = i + 1; j < el->ordNumb; j++) {
        o = &demand[el->ordIDs[j - 1]];
        o2 = &demand[el->ordIDs[j]];
        from = el->ordActions[j - 1] == 'i' ? o->fromStand : o->toStand;
        to = el->ordActions[j] == 'i' ? o2->fromStand : o2->toStand;
        if (from != to) { 
          dst += dist(from, to) + STOP_WAIT;
        }
        if (el->ordIDs[j] == el->ordIDs[i]) { // you don't need to check 'o', it has to be it
          sum += (dst - o->distance); // actual distance - distance without pool
          break;
        }
      }
    } 
  }
  return sum;
}

int countDistanceWithoutPassengers(Branch *el) {
  int count = 0;
  int dst = 0;
  Order *o, *o2;
  int from, to;
  for (int i = 0; i < el->ordNumb - 2; i++) { // normaly it would be -1, but we know that the last leg cannot be empty
    if (el->ordActions[i] == 'i') {
      count++;
    } else count --; // 'o'
    if (count == 0) { // now check if the leg is movement, if so - add distance
      o = &demand[el->ordIDs[i]];
      o2 = &demand[el->ordIDs[i + 1]];
      from = el->ordActions[i] == 'i' ? o->fromStand : o->toStand;
      to = el->ordActions[i + 1] == 'i' ? o2->fromStand : o2->toStand;
      if (from != to) { 
        dst += dist(from, to) + STOP_WAIT;
      }
    }
  }
  return dst;
}

/// check if passengers in pool 'x' exist in pool 'y'
boolean isFound(Branch *br1, Branch *br2, int size) {   
    for (int x = 0; x < size; x++)
      if (br1->ordActions[x] == 'i') 
        for (int y = 0; y < size; y++) 
          if (br2->ordActions[y] == 'i' && br2->ordIDs[y] == br1->ordIDs[x])
            return true;
    return false;
}

int findNearestCab(int from, int pass_count) {
    int dst = 10000; // big enough
    int nearest = -1;
    int found_any = 0;
    for (int i = 0; i < cabsNumb; i++) {
      if (supply[i].id == -1) // allocated earlier to a pool
        continue;
      found_any = 1;
      if (dist(supply[i].location, from) < dst && supply[i].seats >= pass_count) {
        dst = dist(supply[i].location, from);
        nearest = i;
      }
    }
    if (!found_any) return -1; // no cabs at all
    else if (nearest == -1) return -2;  // there are some cabs available but none with so many seats
    return nearest;
}

void findPool(int inPool, int numbThreads) {
    if (inPool > MAXINPOOL) {
      return;
    }
    for (int i=0; i<MAXNODE; i++) nodeSize[i] = 0;
    for (int i=0; i<NUMBTHREAD; i++) nodeSizeSMP[i] = 0;
    dive(0, inPool, numbThreads);

    // debug, to identify needed memory
    for (int i = 0; i < inPool + inPool - 1; i++)
        printf("node[%d].size: %d\n", i, countNodeSize(i));
    rmDuplicatesAndFindCab(inPool);
    printf("FINAL: inPool: %d, found pools: %d\n", inPool, countNodeSize(0));
}
