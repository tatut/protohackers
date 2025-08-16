#!/bin/sh


rm -rf solution*
cc -fsanitize=address -g -O0 -std=gnu11 $1 -o solution
#cc -O3 -std=gnu11 $1 -o solution
