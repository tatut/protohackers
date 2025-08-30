#ifndef JSON_H
#define JSON_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#define VA_ARGS(...) , ##__VA_ARGS__
#define panic(fmt, ...) \
  fprintf(stderr, "[ERROR] " fmt "\n" VA_ARGS(__VA_ARGS__)); return false;

bool json_expect(char **at, char CH);
bool json_expect_consume(char **at, char CH);
char json_at(char **at);

bool json_long(char **at, long *value);
bool json_u16(char **at, uint16_t *value);
bool json_u8(char **at, uint8_t *value);
bool json_double(char **at, double *value);
bool json_bool(char **at, bool *value);

void json_skipws(char **at);

/* Read string (of max length n) to given value.
 */
bool json_string(char **at, size_t n, char *value);

/* Read string and return sub pointer to json.
 * Modifies the end of the string to be '\0'.
 */
bool json_string_ptr(char **at, char **value);

/* Read string of max len 64. */
bool json_string_64(char **at, char *value);
/* Read string of max len 512 */
bool json_string_512(char **at, char *value);

/* skip any JSON value */
bool json_skip(char **pos);

/* JSON raw, skip any JSON value but return a copy of its raw data (allocates) */
bool json_raw(char **pos, char **raw);

#define json_field(name, parser, to)                                    \
  if (strcmp(name, _json_field) == 0) {                                 \
    if (!parser(_json_obj, to)) {                                       \
      panic("Failed to parse field: %s", name)                          \
    }                                                                   \
    continue;                                                           \
  }

#define json_field_req(name, parser, to, has_field)                            \
  if (strcmp(name, _json_field) == 0) {                                        \
    if (!parser(_json_obj, to)) {                                              \
      panic("Failed to parse field: %s", name)                                 \
    }                                                                          \
    (has_field) = true;                                                        \
    continue;                                                                  \
  }



#define json_ignore_unknown_fields() if(!json_skip(_json_obj)) { panic("Failed to skip JSON value.") } else { continue; }

#define json_object(in, body)                                                  \
  char **_json_obj = in;                                                       \
  char _json_field[128];                                                       \
  bool _json_first = true;                                                     \
  if (!json_expect_consume(_json_obj, '{'))                                    \
    return false;                                                              \
  while (json_at(_json_obj) != '}') {                                          \
    if (!_json_first)                                                          \
      json_expect_consume(_json_obj, ',');                                     \
    _json_first = false;                                                       \
    json_string(_json_obj, 128, _json_field);                                  \
    /*printf(" at key: %s\n", _json_field); */                                 \
    json_expect_consume(_json_obj, ':');                                       \
    body panic("Unhandled JSON field: %s", _json_field)                        \
  }                                                                            \
  json_expect_consume(_json_obj, '}');                                         \
  in = _json_obj;


#define json_array(in, type, parser)                                           \
  char **_json_arr = in;                                                       \
  bool _json_first = true;                                                     \
  json_expect_consume(_json_arr, '[');                                         \
  while (json_at(_json_arr) != ']') {                                          \
    if (!_json_first)                                                          \
      json_expect_consume(_json_arr, ',');                                     \
    _json_first = false;                                                       \
    type _json_arr_val;                                                        \
    if (!parser(_json_arr, &_json_arr_val)) {                                  \
      panic("Can't parse array value of type: %s", #type);                     \
    }                                                                          \
    json_array_append(_json_arr_val);                                          \
  }                                                                            \
  json_expect_consume(_json_arr, ']');                                         \
  in = _json_arr;






#endif

#ifdef JSON_IMPLEMENTATION
static void skipws(char **at) {
  char c = **at;
  while(c == ' ' || c == '\t' || c == '\n' || c == '\r') {
    *at = *at + 1;
    c = **at;
  }
}

static bool is_alpha(char ch) {
  return (ch >= 'a' && ch <= 'z') ||
    (ch >= 'A' && ch <= 'Z');
}

static bool is_digit(char ch) { return ch >= '0' && ch <= '9'; }

static bool is_alphanumeric(char ch) { return is_alpha(ch) || is_digit(ch); }

static bool looking_at(char *at, char *word) {
  size_t c = 0;
  while(*word != 0) {
    if(*at == 0) return false;
    if(*at != *word) return false;
    at++;
    word++;
    c++;
  }
  return true;
}
static bool looking_at_then(char *at, char *word, char **next) {
  if(looking_at(at, word)) {
    *next = at + strlen(word);
    return true;
  }
  return false;
}

bool json_expect(char **at, char ch) {
  skipws(at);
  return **at == ch;
}

bool json_expect_consume(char **at, char ch) {
  skipws(at);
  if(**at != ch) return false;
  *at = *at + 1;
  return true;
}

char json_at(char **at) {
  skipws(at);
  return **at;
}

bool json_long(char **pos, long *value) {
  skipws(pos);
  char *at = *pos;
  if(!is_digit(*at) && *at != '-') return false;
  long s = 1;

  if(*at == '-') {
    s = -1;
    at++;
  }
  long num = 0;
  while(is_digit(*at)) {
    num *= 10;
    num += *at - '0';
    at++;
  }
  *value = num * s;
  *pos = at;
  return true;
}

bool json_u16(char **pos, uint16_t *value) {
  long v;
  if(!json_long(pos, &v)) return false;
  if(v <= UINT16_MAX) {
    *value = (uint16_t) v;
    return true;
  }
  return false;
}

bool json_u8(char **pos, uint8_t *value) {
  long v;
  if(!json_long(pos, &v)) return false;
  if(v <= UINT8_MAX) {
    *value = (uint8_t) v;
    return true;
  }
  return false;
}


bool json_double(char **pos, double *value) {
  long longval;
  if(!json_long(pos, &longval)) return false;
  char *at = *pos;
  if(*at == '.') {
    at++;
    // fraction
    double fr = 1;
    double frac = 0;
    while(is_digit(*at)) {
      fr *= 10;
      frac = 10*frac + (*at - '0');
      at++;
    }
    *pos = at;
    double f = longval >= 0 ? 1.0 : -1.0;
    *value = (double) longval + (f * frac/fr);
  } else {
    // integer part only
    *value = (double) longval;
  }
  return true;
}

static int hex(char ch) {
  if(ch >= '0' && ch <= '9') return ch - '0';
  if(ch >= 'A' && ch <= 'F') return ch - ('A' - 10);
  if(ch >= 'a' && ch <= 'f') return ch - ('a' - 10);
  return -1;
}

typedef struct {
  bool success;
  char *start;
  size_t len;
  char *after;
} Str;

static Str parse_string(char *at) {
  char *r, *w;
  // Naive first attempt, just take bytes until '"'
  char *end = at+1;
  while(*end != '"') {
    if(*end == '\\') goto handle_escape;
    end++;
  }
  return (Str) { true, at, end-at, end+1 };

 handle_escape:
  /* handle escapes, mutates input char*, keep track of read and write
   * pointers.
   */
  r = end;
  w = end;
  while(*r != '"') {
    if(*r == '\\') {
      r++;
      switch(*r) {
      case '"': r++; *w = '"'; w++; break;
      case 't': r++; *w = '\t'; w++; break;
      case 'n': r++; *w = '\n'; w++; break;
      case '\\':r++; *w = '\\'; w++; break;
      case '/': r++; *w = '/'; w++; break;
      case 'b': r++; *w = '\b'; w++; break;
      case 'f': r++; *w = '\f'; w++; break;
      case 'u': {
        char hex[5] = { *(r+1), *(r+2), *(r+3), *(r+4), 0 };
        char *_end;
        long codepoint = strtol(hex, &_end, 16);

        if(codepoint <= 127) {
          // single utf-8 byte
          *w = codepoint;
          w++;
        } else if(codepoint <= 2047) {
          // 2 bytes
          *w = 0b11000000 + (0b00011111 & (codepoint>>6));
          w++;
          *w = 0b10000000 + (0b00111111 & codepoint);
        } else if(codepoint <= 65535) {
          *w = 0b11100000 + (0b00001111 & (codepoint>>12));
          w++;
          *w = 0b10000000 + (0b00111111 & (codepoint>>6));
          w++;
          *w = 0b10000000 + (0b00111111 & codepoint);
          w++;
        } else if(codepoint <= 1114111) {
          *w = 0b11110000 + (0b00000111 & (codepoint>>18));
          w++;
          *w = 0b10000000 + (0b00111111 & (codepoint>>12));
          w++;
          *w = 0b10000000 + (0b00111111 & (codepoint>>6));
          w++;
          *w = 0b10000000 + (0b00111111 & codepoint);
          w++;
        }
        r += 5;

      }
      }
    } else {
      *w = *r;
      w++; r++;
    }
  }
  return (Str) { true, at, w-at, r+1 };

 fail:
  return (Str) { false, 0, 0, 0 };

}

// read simple '"' delimited string of maxlen
bool json_string(char **pos, size_t maxlen, char *value) {
  if(!json_expect_consume(pos, '"')) return false;
  char *at = *pos;

  Str s = parse_string(at);
  if(s.success) {
    if(s.len < maxlen-1) {
      memcpy(value, s.start, s.len);
      value[s.len] = 0;
      *pos = s.after;
      return true;
    } else {
      panic("Not enough space for string: %zu < %zu", maxlen, s.len);
    }
  }
  return false;
}

bool json_string_64(char **pos, char *value) {
  return json_string(pos, 64, value);
}
bool json_string_512(char **pos, char *value) {
  return json_string(pos, 512, value);
}

bool json_string_ptr(char **pos, char **value) {
  if(!json_expect_consume(pos, '"')) return false;
  char *at = *pos;

  Str s = parse_string(at);
  if(s.success) {
    *(s.start + s.len) = 0;
    *value = s.start;
    *pos = s.start + s.len + 1;
    return true;
  }
  return false;
}

void json_skipws(char **pos) { skipws(pos); }

/* Skip a single valid json value */
bool json_skip(char **pos) {
  skipws(pos);
  double f;
  Str s;
  bool first=true;

  //dbg("skipping json, pointer at: %p, char: %c", *pos, **pos);
  switch(**pos) {
  case '-':
  case '0': case '1': case '2': case '3': case'4':
  case '5': case '6': case '7': case '8': case '9':
    return json_double(pos, &f);

  case '"':
    s = parse_string(*pos + 1);
    if(s.success) {
      *pos = s.after;
      return true;
    } else {
      return false;
    }

  case '{': {
    *pos += 1;
    while(json_at(pos) != '}') {
      if(!first) {
        if(!json_expect_consume(pos, ',')) return false;
      }
      first = false;
      if(!json_expect_consume(pos, '"')) return false;
      s = parse_string(*pos);
      if(!s.success) return false;
      *pos = s.after;
      if(!json_expect_consume(pos, ':')) return false;
      if(!json_skip(pos)) return false; // skip the value
    }
    return json_expect_consume(pos, '}');
  }

  case '[': {
    *pos += 1;
    while(json_at(pos) != ']') {

      if(!first) {
        if(!json_expect_consume(pos, ',')) return false;
      }
      first = false;
      if(!json_skip(pos)) return false;
    }
    return json_expect_consume(pos, ']');
  }
  default:
    if(looking_at(*pos, "null") && !is_alphanumeric((*pos)[4])) {
      *pos = *pos + 4;
      return true;
    }
    if(looking_at(*pos, "true") && !is_alphanumeric((*pos)[4])) {
      *pos = *pos + 4;
      return true;
    }
    if(looking_at(*pos, "false") && !is_alphanumeric((*pos)[5])) {
      *pos = *pos + 5;
      return true;
    }
    return false;
  }
  return false;
}

bool json_raw(char **pos, char **raw) {
  skipws(pos);
  char *start = *pos;
  if(json_skip(pos)) {
    size_t size = *pos - start + 1;
    char *data = malloc(size);
    if(!data) {
      panic("Couldn't allocate %ld bytes of memory for raw JSON object.", size);
    }
    memcpy(data, start, size-1);
    data[size-1] = 0;
    *raw = data;
    return true;
  }
  return false;
}

bool json_bool(char **pos, bool *b) {
  if(looking_at(*pos, "true") && !is_alphanumeric((*pos)[4])) {
    *b = true;
    *pos = *pos + 4;
    return true;
  } else if(looking_at(*pos, "false") && !is_alphanumeric((*pos)[5])) {
    *b = false;
    *pos = *pos + 5;
    return true;
  }
  return false;
}


#endif
