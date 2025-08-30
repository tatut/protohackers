#!/bin/sh


rm -rf solution*
cc -DDEBUG \
   -fsanitize=address \
   -fsanitize-address-use-after-return=runtime \
   -g -O0 -std=gnu11 $1 -o solution
#cc -O3 -std=gnu11 $1 -o solution
