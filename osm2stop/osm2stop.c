#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX 100

char value[MAX];

void read_value(char * ptr) {
  int j = 0;
  memset(value, 0, MAX);
  while (*ptr!= '"') {
    value[j++] = *ptr;
    if (j == MAX) {
      printf("Max string is exceede\n");
      exit(0);
    }
    ptr++;
  }
  value[j]=0;
}

// cc osm2stop.c -Wno-implicit-function-declaration -w -o osm2stop
// b --line 87 -c '(int)strcmp("Mariensztat 8 Apartments", value)==0'
int main(int argc, char *argv[]) {
    char ch, *arr, *str, *tag, *ptr, *ptr2, *atr, *lon;
    double flat = 0, flon = 0;
    arr = malloc(2000000000);
    str = arr;
    int i = 0, j, k;
    FILE *fptr = fopen(argc == 2 ? argv[1] :"/Users/bogusz.jelinski/eclipse/scenario-convert-25.0/Berlin.osm", "r");
    while ((ch = fgetc(fptr)) != EOF)
        arr[i++] = ch;
    fclose(fptr);

    while(tag = strstr(str, "<tag k=\"highway\" v=\"bus_stop\"/>")) {
        // get back and find lat/lon
        ptr = tag - 1; 
        k = 0;
        
        while (*ptr != 'n' || *(ptr+1) != 'o' || *(ptr+2) != 'd' || *(ptr+3) != 'e') {
          ptr--;
          k++;
          if (k>2000) { // just in case
            printf("Node not found\n");
            exit(0);
          }
        }
        // find lat
        atr = strstr(ptr, "lat=\"");
        if (atr == NULL) {
            printf("Latitude not found\n");
            exit(0);
        }
        atr += 5; // go to value and copy
        read_value(atr);
        sscanf(value, "%lf", &flat); 
        // find long
        atr = strstr(ptr, "lon=\"");
        if (atr == NULL) {
            printf("Longitude not found\n");
            exit(0);
        }
        atr += 5; // go to value
        read_value(atr);
        sscanf(value, "%lf", &flon); 

        // find name, start with <node as we don't assume the order of tags
        atr = strstr(ptr, "<tag k=\"name\" v=\"");
        if (atr == NULL) {
            printf("Name not found\n");
            exit(0);
        }
        // but check if the name is within the same node
        ptr2 = ptr + 4;
        int found_node = 0;
        while (ptr2 != atr) {
          if (*ptr2 == 'n' && *(ptr2+1) == 'o' && *(ptr2+2) == 'd' && *(ptr2+3) == 'e') {
            // nope, end of node here
            found_node = 1;
            break;
          }
          ptr2++;
        }
        if (!found_node) { // !found = within the same node
          atr += 17; // go to value
          j = 0;
          while (*atr!= '"') {
            value[j++] = *atr;
            if (j == MAX) {
              printf("Max string is exceede\n");
              exit(0);
            }
            atr++;
          }
          value[j]=0;
          printf("%s %.8lf %.8lf\n", value, flat, flon);
        } 
        // move str after bus_stop tag
        str = tag + 30; // go beyond bus_stop
    }
    free(arr);
    return 0;
}
