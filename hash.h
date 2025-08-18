#ifndef hash_h
#define hash_h
#include <stdint.h>
unsigned long hash(uint8_t *data, size_t len);
#endif
#ifdef HASH_IMPLEMENTATION
// modified djb2 hash: http://www.cse.yorku.ca/~oz/hash.html
unsigned long hash(uint8_t *data, size_t len) {
  unsigned long hash = 5381;
  int c;

  for(size_t i=0;i<len;i++) {
    c = *data++;
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
  }
  return hash;
}
#endif
