#include <stdint.h>
#define SERVER_IMPLEMENTATION
#include "server.h"

typedef struct price {
  int32_t ts; // seconds since epoch
  int32_t price; // price in pennies
} price;

int compare_by_ts(const void *a, const void *b) {
  return ((price*)a)->ts - ((price*)b)->ts;
}

void means_to_an_end(int socket, void *_data) {
  uint8_t msg[9];
  int r;
  price *p;

  price *prices = malloc(100*sizeof(price));
  int count=0;
  int capacity=100;

  bool sorted=true;

  while(1) {
    if((r = recv(socket, msg, 9, MSG_WAITALL)) < 9) {
      for(int i=0;i<r;i++) printf(" read byte: %d (%c)\n", msg[i], msg[i]);
      err("read %d, closing socket %d", r, socket);
      break;
    }
    switch(msg[0]) {
    case 'I': {
      int32_t ts = ntohl(*((int32_t*)&msg[1]));
      int32_t pr = ntohl(*((int32_t*)&msg[5]));
      if(count == capacity) {
        capacity = capacity * 1.618; // increase by golden ratio
        prices = realloc(prices, capacity * sizeof(price));
        if(!prices) {
          err("Can't realloc to %d * sizeof(price)", capacity);
          return;
        }
      }
      prices[count++] = (price) {.ts = ts, .price = pr};
      sorted = false;
      dbg("(%d) Insert: ts=%d, price=%d", count, ts, pr);
      break;
    }
    case 'Q': {
      int32_t min_ts = ntohl(*((int32_t*)&msg[1]));
      int32_t max_ts = ntohl(*((int32_t*)&msg[5]));
      int64_t total = 0;
      int32_t c = 0;
      price *at = p;
      if(!sorted) {
        qsort(prices, count, sizeof(price), compare_by_ts);
        sorted = true;
      }

      int i=0;
      while(i < count && prices[i].ts < min_ts) i++;
      while(i < count && prices[i].ts <= max_ts) {
        total += prices[i++].price;
        c++;
      }
      dbg("Query: min_ts=%d,max_ts=%d => total=%lld,count=%d,average=%d",
          min_ts, max_ts, total, c, (int32_t) (total/c));
      int32_t avg = htonl(total/c);
      write(socket, &avg, 4);
      break;
    }
    default:
      err("Unexpected message type: %c, closing.", msg[0]);
      goto done;
    }
  }

  done:
  // free measurements
  free(prices);
}


int main(int argc, char **argv) {
  serve(.handler = means_to_an_end);
}
