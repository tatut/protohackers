#include <stdint.h>
#define SERVER_IMPLEMENTATION
#include "server.h"


typedef uint32_t u32;
typedef uint8_t u8;

u8 reversebits(u8 b) {
   b = (b & 0xF0) >> 4 | (b & 0x0F) << 4;
   b = (b & 0xCC) >> 2 | (b & 0x33) << 2;
   b = (b & 0xAA) >> 1 | (b & 0x55) << 1;
   return b;
}

uint8_t apply(u8 *cipher, u8 in, size_t pos, bool inverse) {
  size_t c=0;
  u8 orig = in;
  while(cipher[c] != 0) {
    //printf("op: %d, pos: %ld,   in: %d (0x%x)", cipher[c], pos, in, in & 0xff);
    switch(cipher[c++]) {
    case 1: in = reversebits(in); break;
    case 2: in = in ^ cipher[c++]; break;
    case 3: in = in ^ (u8)(pos%256); break;
    case 4: {
      if(inverse) {
        in = (u8) (((int)in - (int)cipher[c++]) % (int)256);
      } else {
        in = (u8) (((u32) in + (u32) cipher[c++]) % 256);
      }
      break;
    }
    case 5: {
      if(inverse) {
        //dbg("in: %d, pos: %ld => %d", in, pos, (u8) (((int)in - (int)pos) % 256));
        in = (u8) (((int)in - (int)pos) % (int)256);
      } else {
        in = (u8) ((((u32) in + (u32)pos)) % 256);
      }
      break;
    }
    default:
      err("Unrecognized cipher op: %d", cipher[c-1]);
      return 0;
    }
    //printf(" => %d (0x%x)\n", in, in % 0xff);
  }
  //  printf("(%d -> %d)\n", orig, in);
  //fflush(stdout);
  return in;
}

size_t cipher_read(conn_state *s, uint8_t *cipher) {
#define NEXT() do { if(read(s->socket, &byte, 1) != 1) return false;  dbg("  read cipher byte: %d", byte); count++; } while(false)
  uint8_t byte;
  int count=0;
  NEXT();
  while(byte != 0) {
    switch(byte) {
    case 1: case 3: case 5: {
      *cipher++ = byte;
      break;
    }
    case 2: case 4: {
      *cipher++ = byte;
      NEXT();
      *cipher++ = byte;
      break;
    }
    default:
      err("Unexpected cipher op: %d", byte);
      return 0;
    }
    NEXT();
  }
  *cipher = 0;
  dbg("got cipher of %d bytes", count);
  return count;
}

void cipher_invert(u8 *cipher, u8 *inverted, size_t len) {
  size_t i=0;
  size_t ipos = len-1;
  inverted[ipos] = 0;
  while(cipher[i] != 0) {
    switch(cipher[i]) {
    case 1: case 3: case 5:
      inverted[--ipos] = cipher[i++];
      break;
    case 2: case 4:
      inverted[ipos-2] = cipher[i];
      inverted[ipos-1] = cipher[i+1];
      ipos -= 2;
      i += 2;
      break;
    default:
      err("Unexpected cipher op: %d", cipher[i]);
    }
  }
}
void test_cipher(uint8_t *cipher, uint8_t *data_in, size_t len, size_t pos, bool inverse) {
  uint8_t *data = alloca(len);
  memcpy(data, data_in, len);
  printf("before:\n");
  for(int i=0;i<len;i++) {
    if(inverse)
      printf("  %x  ", data[i] & 0xff);
    else
      printf("%c %x  ", data[i], data[i] & 0xff);
  }
  for(int i=0;i<len;i++) {
    data[i] = apply(cipher, data[i], pos+i, inverse);
  }
  printf("\nafter:\n");
  for(int i=0;i<len;i++) {
    if(inverse)
      printf("%c %x  ", data[i], data[i] & 0xff);
    else
      printf("  %x  ", data[i] & 0xff);
  }
  printf("\n------\n");
}

bool read_byte(conn_state *s, uint8_t *to) {
  uint8_t byte;
  if(read(s->socket, &byte, 1) != 1) return false;
  *to = byte;
  return true;
}

typedef struct str {
  ptrdiff_t len;
  char *data;
} str;

bool split(str in, char sep, str *part1, str *rest) {
  if(in.len == 0) return false;
  int p=0;
  while(p < in.len && in.data[p] != sep) p++;
  if(p == in.len) {
    *part1 = in;
    *rest = (str) {.len = 0, .data = NULL };
  } else {
    *part1 = (str) { .len = p, .data = in.data };
    *rest = (str) { .len = in.len - p -1 , .data = in.data+p+1};
  }
  return true;
}

str handle_line(str line) {
  dbg("GOT LINE: %.*s", (int) line.len, line.data);
  int how_many = -1;
  str result;

  str part, rest=line;
  while(split(rest, ',', &part, &rest)) {
    dbg("GOT PART: %.*s", (int) part.len, part.data);
    int n = 0;
    for(int i=0;i<part.len && part.data[i] != 'x'; i++)
      n = n * 10 + (part.data[i] - '0');
    dbg(" times: %d", n);
    if(how_many < n) {
      how_many = n;
      result = part;
    }
  }
  if(how_many == -1)
    result = line;

  return result;
}

// brute force! :(
bool is_nop(u8* cipher) {
  for(int b=0;b<=255;b++) {
    for(size_t pos=0;pos<256;pos++) {
      if(b != apply(cipher, (u8) b, pos, false)) return false;
    }
  }
  return true;
}

void orders(conn_state *s) {
  dbg("New connection");
  uint8_t cipher[80], inverted[80];
  uint8_t buf[5001];
  size_t in_pos=0, out_pos=0;
  size_t line_pos=0;
  uint8_t b;

  size_t cipher_len = cipher_read(s, cipher);
  if(!cipher_len) return;
  if(cipher_len == 1 || is_nop(cipher)) {
    dbg("No op cipher, disconnecting.");
    return;
  }

  cipher_invert(cipher, inverted, cipher_len);
  for(int i=0;i<cipher_len; i++) {
    dbg("  inverted: %d", inverted[i]);
  }

  while(read_byte(s, &b)) {
    b = apply(inverted, b, in_pos++, true);
    if(b == '\n') {
      str result = handle_line((str){.len=line_pos, .data=(char*)buf});
      for(size_t i=0;i<result.len;i++) {
        uint8_t b = apply(cipher, result.data[i], out_pos++, false);
        write(s->socket, &b, 1);
      }
      uint8_t b = apply(cipher, '\n', out_pos++, false);
      write(s->socket, &b, 1);
      line_pos = 0;
    } else {
      buf[line_pos++] = b;
    }
  }
  dbg("Client done!");
}

int main(int argc, char **argv) {
  serve(.type = SERVER_THREAD_WORKERS, .handler = orders);

}
