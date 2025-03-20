#!/bin/bash
rm libdynapool.so
cc -O3 -Wno-implicit-function-declaration -shared -o libdynapool.so -w poold.c dynapool.c
cc tests.c -o tests -ldynapool -L .
./tests