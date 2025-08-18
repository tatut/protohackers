#ifndef arena_h
#define arena_h
#include <stdalign.h>
#include <stdint.h>
#include <stddef.h>

/* == Arena allocator == */
typedef struct arena {
  uint8_t *start;   // where allocated memory starts
  uint8_t *cur;     // current start of unused memory
  uint8_t *end;     // start of memory after slab
} arena;

#define ARENA_DEFAULT_SIZE (8 * 1024 * 1024) // 8mb default size

#define new(arena, type, count)                                                \
  (type *)arena_alloc(arena, sizeof(type), alignof(type), count)


/* Allocate memory with alignment */
void *arena_alloc(arena *arena, ptrdiff_t size, ptrdiff_t align,
                      ptrdiff_t count);
/* Initialize arena with given capacity */
void arena_init(arena *arena, size_t capacity);

/* Free the arena memory */
void arena_free(arena *arena);

/* Clear arena so the memory can be reused */
void arena_clear(arena *arena);

/* Copy string to arena */
char *arena_str(arena *arena, char *str);
#endif

#ifdef ARENA_IMPLEMENTATION
void *arena_alloc(arena *arena, ptrdiff_t size, ptrdiff_t align,
                      ptrdiff_t count) {
  if(!arena->start) {
    arena_init(arena, ARENA_DEFAULT_SIZE);
  }
  ptrdiff_t pad = -(uintptr_t)arena->cur & (align - 1);
  if(arena->cur + pad + (count * size) >= arena->end) {
    error("Out of arena memory, tried to allocate %ld (pad %ld)", (count*size), pad);
    return NULL;
  } else {
    void *p = arena->cur + pad;
    //printf("allocated pointer of %llu (size: %ld, count: %ld, align: %ld, pad: %ld)\n", (uint64_t) p, size, count, align,pad);
    memset(p, 0, count * size);
    //printf("memset done\n");
    arena->cur += pad + (count * size);
    return (void *)p;
  }
}

/**
 * Init a new arena by allocating memory for it via malloc.
 */
void arena_init(arena *arena, size_t capacity) {
  arena->start = (uint8_t *) malloc(capacity);
  arena->cur = arena->start;
  arena->end = arena->start + capacity;
}

void arena_free(arena *arena) {
  if(arena->start != NULL) {
    free(arena->start);
    arena->start = NULL;
    arena->cur = NULL;
    arena->end = NULL;
  }
}

void arena_clear(arena *arena) {
  arena->cur = arena->start;
}

char *arena_str(arena *arena, char *str) {
  if(str == NULL) return NULL;
  size_t len = strlen(str)+1;
  char *out = (char*) arena_alloc(arena, len, 1, 1);
  if(!out) return NULL;
  memcpy(out, str, len);
  return out;
}

#endif
