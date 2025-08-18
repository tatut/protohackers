#include <stdint.h>
#define SERVER_IMPLEMENTATION
#include "server.h"
#define HASH_IMPLEMENTATION
#include "hash.h"

#define HT_SIZE 4096

typedef struct KV {
  uint8_t *key;
  uint8_t *val;
  // udp can't have longer lengths
  uint16_t keylen, vallen;
} kv;

typedef struct bucket {
  kv *items;
  int count;
  int capacity;
} bucket;

bucket table[HT_SIZE] = {0};

void respond(dgram *d, uint8_t *data, size_t len) {
  sendto(d->socket, data, len, 0,
         (struct sockaddr *)d->from, sizeof(struct sockaddr_in));
}


void kv_database(dgram *d) {
  dbg("GOT: (%d) \"%.*s\"", d->len, d->len, d->data);
  if(d->len == 7 && memcmp(d->data, "version", 7)==0) {
    respond(d, (uint8_t*)"version=tatut", 13);
    return;
  }

  uint8_t *key = d->data, *val = NULL;
  uint16_t keylen = d->len, vallen = 0;

  bool has_val=false;
  for(size_t i=0;i<d->len;i++) {
    if(d->data[i] == 61) {
      keylen = i;
      val = &d->data[i+1];
      vallen = d->len - keylen - 1;
      has_val = true;
      break;
    }
  }

  if(has_val) {
    // this is storage
    dbg("Store %.*s (%d) = %.*s (%d)", keylen, key, keylen, vallen, val, vallen);
    long idx = hash(key, keylen) % HT_SIZE;

    // go through bucket to find value
    bucket *b = &table[idx];
    dbg("found bucket %ld with count %d", idx, b->count);
    for(int i=0;i<b->count;i++) {
      dbg("compare %.*s with %.*s", b->items[i].keylen, b->items[i].key, keylen, key);
      if(b->items[i].keylen == keylen && memcmp(b->items[i].key,key,keylen)==0) {
        // overwrite
        dbg("Overwrite key \"%.*s\" with new value: \"%.*s\"", keylen, key, vallen, val);
        free(b->items[i].val);
        b->items[i].vallen = vallen;
        b->items[i].val = malloc(vallen);
        if(!b->items[i].val) {
          err("Out of memory");
          exit(1);
        }
        memcpy(b->items[i].val, val, vallen);
        return;
      }
    }
    dbg("Add key \"%.*s\" with value: \"%.*s\"", keylen, key, vallen, val);
    // add new value to bucket
    if(b->capacity == b->count) {
      int new_capacity = b->capacity == 0 ? 8 : (b->capacity * 1.618);
      b->items = realloc(b->items, new_capacity*sizeof(kv));
      if(!b->items) {
        err("Failed to realloc bucket");
        exit(1);
      }
      b->capacity = new_capacity;
    }
    b->items[b->count] = (kv) {.key = malloc(keylen), .keylen = keylen,
                                 .val = malloc(vallen), .vallen = vallen};
    if(!b->items[b->count].key || !b->items[b->count].val) {
      err("Failed to allocate new item");
      exit(1);
    }
    memcpy(b->items[b->count].key, key, keylen);
    memcpy(b->items[b->count].val, val, vallen);
    b->count++;
  } else {
    // this is query
    dbg("Query: \"%.*s\"", keylen, key);
    long idx = hash(key, keylen) % HT_SIZE;
    bucket b = table[idx];
    uint8_t buf[1024];
    memcpy(buf, key, keylen);
    buf[keylen] = '=';
    size_t len = keylen+1;
    for(int i=0;i<b.count;i++) {
      if(b.items[i].keylen==keylen && memcmp(b.items[i].key,key,keylen)==0) {
        dbg("Found key %.*s with value %.*s", keylen,key, b.items[i].vallen, b.items[i].val);
        memcpy(&buf[len], b.items[i].val, b.items[i].vallen);
        len += b.items[i].vallen;
        break;
      }
    }
    dbg("sending %.*s", (int)len, buf);
    respond(d, buf, len);
  }
}

int main(int argc, char **argv) {
  serve(.type = SERVER_DGRAM, .dgram_handler=kv_database);
}
