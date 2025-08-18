#ifndef dynarr_h
#define dynarr_h
#include <assert.h>
typedef struct {
  char *items;
  uint32_t size, capacity;
} _Arr;

#define DefineArray(type, item_type)                                    \
  typedef struct {                                                      \
    item_type *items;                                                   \
    uint32_t size, capacity;                                            \
  } type;

void _da_ensure(_Arr *a, uint32_t data_size);

#define DA_INITIAL_CAPACITY 8
#define DA_GROWTH_FACTOR 1.618 // golden ratio

#define da_init(arr) { (arr)->items = NULL; (arr)->capacity = 0; (arr)->size = 0; }

#define da_append(arr, item)                                           \
  _da_ensure((_Arr*)(arr), sizeof(*((arr)->items)));                  \
  (arr)->items[(arr)->size++] = (item);

#define da_lastptr(type, arr) (type*)&(arr)->items[arr->size - 1]


#define da_get(type, arr, i) (type)(((type *)((arr)->items))[i])
#define da_getp(type, arr, i) &(((type *)((arr)->items))[i])

/* Fast by moving last item in place of deleted one.
 * Use when you don't need to maintain order. */
#define da_del(arr, i)                                                        \
  if ((i) == (arr).size - 1) {                                                 \
    (arr).size--;                                                              \
  } else if ((i) >= 0 && (i) < (arr).size - 1) {                               \
    (arr).items[i] = (arr).items[(arr).size - 1];                              \
    (arr).size--;                                                              \
  }

#define da_foreach(type, item, arr, body)                                     \
  {                                                                            \
    for (size_t item##_it = 0; item##_it < (arr)->size; item##_it++) {   \
      type item = da_get(type, arr, item##_it);                               \
      body                                                                     \
    }                                                                          \
  }

#define da_free(arr) { if((arr)->items) free((arr)->items); (arr)->items = NULL; (arr)->capacity = 0; (arr)->size = 0; }


void _da_front(_Arr *arr, uint32_t i, uint32_t data_size);
#endif

#ifdef DYNARR_IMPLEMENTATION

void _da_front(_Arr *arr, uint32_t i, uint32_t data_size) {
  uint8_t buf[128];
  assert(data_size <= 128);
  if(i > 0 && i < arr->size) {
    memcpy(buf, arr->items, data_size);
    memcpy(arr->items, arr->items + (i * data_size), data_size);
    memcpy(arr->items + (i * data_size), buf, data_size);
  }
}

void _da_ensure(_Arr *a, uint32_t data_size) {
  if(a->size < a->capacity) return;
  uint32_t new_capacity = a->capacity == 0 ? DA_INITIAL_CAPACITY : (a->capacity * DA_GROWTH_FACTOR);
  a->items = realloc(a->items, new_capacity * data_size);
  if(!a->items) {
    panic("Could not grow dynamic array, from %u to %u", a->size, new_capacity);
  }
//dbg("dyn array new capacity: %u (%u bytes)", new_capacity, new_capacity*data_size);
  a->capacity = new_capacity;
}

#endif
