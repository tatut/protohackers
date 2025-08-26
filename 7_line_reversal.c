#include <stddef.h>
#include <stdio.h>
#define SERVER_IMPLEMENTATION
#include "server.h"

#define STB_DS_IMPLEMENTATION
#include "stb/stb_ds.h"

#include <sys/time.h>

#define BUF_SIZE 1024

typedef enum { RECEIVING, SENDING } SessionState;
typedef struct Session {
  SessionState state;
  // data for receiving
  int received, received_acked, bytes_available, read_idx;
  uint8_t *rcv_buf;
  uint8_t *snd_buf;

  long last_msg_time; // for detecting timeout

  pthread_mutex_t read_lock;
  pthread_cond_t read_available;

  // data for sending
  pthread_mutex_t write_lock;
  pthread_cond_t write_available;
  int sent, sent_acked, write_idx;
  long last_sent_time;

  // return info
  int socket;
  struct sockaddr_in from;

  FILE *log;
} Session;

typedef struct SessionMap {
  int key;
  Session *value;
} SessionMap;

typedef struct str {
  ptrdiff_t len;
  char *data;
} str;

/* current time in millis */
long now() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
}

bool split(str in, char sep, str *part1, str *rest) {
  int p=0;
  while(p < in.len && in.data[p] != sep) p++;
  if(p == in.len) return false;
  *part1 = (str) { .len = p, .data = in.data };
  *rest = (str) { .len = in.len - p -1 , .data = in.data+p+1};
  return true;
}

// split all, allocs a new dynamic array
str *split_all(str in, char sep) {
  str *parts = NULL;
  str part, rest;
  while(split(in, sep, &part, &rest)) {
    arrput(parts, part);
    in = rest;
  }
  // last part
  if(in.len)
    arrput(parts, in);
  return parts;
}

bool str_eq(str a, str b) {
  if(a.len != b.len) return false;
  return memcmp(a.data,b.data,a.len)==0;
}
#define cstr(X)                                                                \
  (str) { .data = (X), .len = strlen((X)) }

bool str_to_u32(str in, uint32_t *to) {
  uint32_t n = 0;
  for(int i=0;i<in.len;i++) {
    char c = in.data[i];
    if(c < '0' || c > '9') return false;
    n = n * 10 + (c - '0');
  }
  *to = n;
  return true;
}

SessionMap *sessions;

void session_send(Session *s, str data, int line) {
  fprintf(s->log, ">> (%d) %.*s\n", (int) data.len, (int) data.len, data.data);
  fflush(s->log);
  printf("L:%d >> (%d) \"%.*s\"\n", line, (int) data.len, (int)data.len, data.data);
  sendto(s->socket, data.data, data.len, 0, (struct sockaddr*)&s->from, sizeof(struct sockaddr_in));
}
void dgram_send(dgram *dg, str data, int line) {
  printf("L:%d >> (%d) \"%.*s\"\n", line, (int) data.len, (int)data.len, data.data);
  sendto(dg->socket, data.data, data.len, 0, (struct sockaddr*)dg->from, sizeof(struct sockaddr_in));
}
#define SEND(s, fmt, ...) session_send((s), (str) {.data=out, .len=snprintf(out, 1024, fmt, __VA_ARGS__)}, __LINE__)
#define SENDD(d, fmt, ...) dgram_send((d), (str) {.data=out, .len=snprintf(out, 1024, fmt, __VA_ARGS__)}, __LINE__)

void lock(pthread_mutex_t *l) {
  int c = 0;
  while(pthread_mutex_trylock(l) != 0) {
    usleep(100000);
    c++;
    if(c % 10 == 0)
      dbg("Thread waiting on lock for %f seconds", c/10.0);
  }
}

#define defer(name,start,end) for(int __##name = (start,0); !__##name; (__##name += 1), end)
#define locking(lck)                                                       \
  defer(mutex, lock(&(lck)), pthread_mutex_unlock(&(lck)))

void ensure_all_handled(Session *s) {
  while(s->bytes_available) {
    locking(s->read_lock) {
      pthread_cond_signal(&s->read_available);
    }
  }
}


// interface given to app level
typedef Session* IO;

int io_getchar(IO io) {
  // wait until there are
  int ch = -1;
  locking(io->read_lock) {
    while(!io->bytes_available) {
      pthread_cond_wait(&io->read_available, &io->read_lock);
    }
    ch = io->rcv_buf[io->read_idx++];
    io->bytes_available--;
  }
  return ch;
}

void io_putchar(IO io, char ch) {
  // if there is no space in outgoing buffer, wait
  locking(io->write_lock) {
    while(io->write_idx > 800)
      pthread_cond_wait(&io->write_available, &io->write_lock);
    io->snd_buf[io->write_idx++] = ch;
  }
}

/* The "app" level program */
void reverse(str s) {
  int i=0,j=s.len-1;
  while(i < j) {
    char tmp = s.data[i];
    s.data[i] = s.data[j];
    s.data[j] = tmp;
    i++; j--;
  }
}
void reverser(IO io) {
  char line[10240];
  int i=0;
  int ch;

  while(1) {
    i = 0;
    while(i < 10240 && (ch=io_getchar(io)) != '\n')
      line[i++] = ch;
    str l = (str) { .len = i, .data = line };
    dbg("APP got line: (%ld) %.*s", l.len, (int)l.len, l.data);
    fprintf(io->log, "APP: (%d) \"%.*s\"\n", (int)l.len, (int)l.len, l.data);
    reverse(l);
    fprintf(io->log, "REV: (%d) \"%.*s\"\n", (int)l.len, (int)l.len, l.data);
    fflush(io->log);
    for(i=0;i<l.len;i++) {
      io_putchar(io, l.data[i]);
    }
    io_putchar(io, '\n');
  }

}


void line_reversal(dgram *dg) {
  char out[1024];

  printf("<< (%d) \"%.*s\"\n", dg->len, dg->len, dg->data);

  if(dg->len < 5 || dg->data[0] != '/') {
    dbg("Invalid message, len: %d, first ch: %c", dg->len, dg->data[0]);
    return;
  }
  str message = (str){.data=(char*)&dg->data[1], .len=dg->len-1};
  str cmd, rest;
  if(!split(message, '/', &cmd, &rest)) {
    err("Could not split command from message");
    return;
  }

  uint32_t key;
  str key_str;
  if(!split(rest, '/', &key_str, &rest)) {
    err("Could not split session");
    return;
  }
  if(!str_to_u32(key_str, &key)) {
    err("Could not parse session id: %.*s", (int)key_str.len, key_str.data);
    return;
  }

  if(!str_eq(cmd, cstr("connect")) && hmgeti(sessions,key) < 0) {
    dbg("No session found: %d", key);
    SENDD(dg, "/close/%d/", key);
    return;
  }

  Session *s = hmget(sessions,key);
  if(s) {
    fprintf(s->log, "<< (%d) %.*s\n", dg->len, dg->len, dg->data);
    fflush(s->log);
    s->last_msg_time = now();
  }
  if(str_eq(cmd, cstr("connect"))) {
    dbg("Connect from: %d:%d, session id: %d", dg->from->sin_addr.s_addr, dg->from->sin_port,
        key);
    if(s) {
      dbg("New connect received for same id: %d", key);
      SEND(s, "/ack/%d/%d/", key, s->received_acked);
      return;
    }
    s = calloc(1, sizeof(Session) + 8192);
    if(!s) {
      err("Could not allocate new session!");
      return;
    }
    s->from = *dg->from;
    s->socket = dg->socket;
    s->rcv_buf = (uint8_t*) s + sizeof(Session);
    s->snd_buf = s->rcv_buf + 4096;
    s->last_msg_time = now();

    char filename[64];
    snprintf(filename, 64, "log%d.txt", key);
    s->log = fopen(filename, "w");
    fprintf(s->log, "<< (%d) %.*s\n", dg->len, dg->len, dg->data);
    fflush(s->log);

    pthread_cond_init(&s->read_available, NULL);
    pthread_mutex_init(&s->read_lock, NULL);
    pthread_cond_init(&s->write_available, NULL);
    pthread_mutex_init(&s->write_lock, NULL);
    pthread_t tid;
    pthread_create(&tid, NULL, (void*)reverser, s);
    SEND(s, "/ack/%d/%d/", key, s->received_acked);
    hmput(sessions, key, s);
  } else if(str_eq(cmd, cstr("data"))) {
    str pos_str;
    if(!split(rest, '/', &pos_str, &rest)) {
      err("Could not split position for data command");
      return;
    }
    uint32_t pos;
    if(!str_to_u32(pos_str, &pos)) {
      err("Could not parse position: %.*s", (int)pos_str.len, pos_str.data);
      return;
    }
    dbg("Got data, session: %d, pos: %d", key, pos);

    if(s->received - pos > 0) {
      SEND(s, "/ack/%d/%d/", key, s->received);
    } else {
      // Read the data
      char data[1024];
      size_t data_len=0;
      int i=0;
      while(i < rest.len-1) {
        if(rest.data[i] == '\\') {
          if(rest.data[i+1] == '/') {
            data[data_len++] = '/';
            i += 2;
            continue;
          } else if(rest.data[i+1] == '\\') {
            data[data_len++] = '\\';
            i += 2;
            continue;
          } else {
            dbg("Unknown escape code '%c', ignoring packet.", rest.data[i+1]);
            return;
          }
        } else if (rest.data[i] == '/') {
          dbg("Unescaped '/' character, ignoring packet.");
          return;
        } else {
          data[data_len++] = rest.data[i++];
        }
      }
      if(rest.data[i] != '/') {
        err("Message doesn't end in '/', got: %c", rest.data[i]);
        return;
      }
      s->received += data_len;
      SEND(s, "/ack/%d/%d/", key, s->received);

      // wait until everything has been handled

      ensure_all_handled(s);
      memcpy(s->rcv_buf, data, data_len);
      s->bytes_available = data_len;
      s->read_idx = 0;
      dbg("making %ld bytes available for app level: \"%.*s\"", data_len, (int) data_len, s->rcv_buf);
      pthread_cond_signal(&s->read_available);

    }
  } else if(str_eq(cmd, cstr("ack"))) {
    str pos_str;
    if(!split(rest, '/', &pos_str, &rest)) {
      err("Could not split position for ack command");
      return;
    }
    uint32_t pos;
    str_to_u32(pos_str, &pos);

    locking(s->write_lock) {
      if(pos <= s->sent_acked) {
        dbg("Got earlier ack for pos %d (but already received higher %d), ignoring.", pos, s->sent_acked);
      } else if(pos > s->sent) {
        dbg("Got ack for more than we have sent %d (only sent %d), ignoring.", pos, s->sent);
      } else if(s->sent == pos) {
        s->sent_acked = pos;
        s->write_idx = 0;
        // can write more now
        pthread_cond_signal(&s->write_available);
      }
    }


  } else if(str_eq(cmd, cstr("close"))) {
    SEND(s, "/close/%d/", key);
    //fclose(s->log);
    //free(s);
    hmdel(sessions, key);
  }

  //end:

}

#define RETRANSMISSION_TIMEOUT 1500
// send replies for active sessions
void sender() {
  char out[1024];
  while(1) {
    for(int i=0;i<hmlen(sessions);i++) {
      usleep(1000);
      uint32_t key = sessions[i].key;
      Session *s = sessions[i].value;
      locking(s->write_lock) {
        if(s->sent_acked < s->sent) {
          // if more than 3s without ack, resend
          long n = now();
          if(n - s->last_sent_time > RETRANSMISSION_TIMEOUT) {
            dbg("NEEDS RESEND");
            char data[1024];
            int data_len = 0;
            for(int i=0;i<s->write_idx; i++) {
              if(s->snd_buf[i] == '/') {
                data[data_len++] = '\\';
                data[data_len++] = '/';
              } else if(s->snd_buf[i] == '\\') {
                data[data_len++] = '\\';
                data[data_len++] = '\\';
              } else {
                data[data_len++] = s->snd_buf[i];
              }
            }
            SEND(s, "/data/%d/%d/%.*s/", key, s->sent_acked, data_len, data);
            s->last_sent_time = n;
          }
        } else if(s->write_idx > 0) {
          dbg("Needs sending %d", s->write_idx);
          char data[1024];
          int data_len = 0;
          for(int i=0;i<s->write_idx; i++) {
            if(s->snd_buf[i] == '/') {
              data[data_len++] = '\\';
              data[data_len++] = '/';
            } else if(s->snd_buf[i] == '\\') {
              data[data_len++] = '\\';
              data[data_len++] = '\\';
            } else {
              data[data_len++] = s->snd_buf[i];
            }
          }

          SEND(s, "/data/%d/%d/%.*s/", key, s->sent_acked, data_len, data);
          s->last_sent_time = now();
          s->sent += s->write_idx;
        } else if((now() - s->last_msg_time) > 60000) {
          dbg("Closing inactive session: %d", key);
          SEND(s, "/close/%d/", key);
          hmdel(sessions, key);
        }
      }
    }
  }
}


int main(int argc, char **argv) {
  pthread_t sender_thread;
  pthread_create(&sender_thread, NULL, (void*)sender, NULL);

  serve(.type = SERVER_DGRAM, .dgram_handler = line_reversal);

  /*
  char helloworld[12] = "hello world";
  str hello = (str) {.len = 11, .data = helloworld };
  reverse(hello);
  printf("reversed: \"%.*s\"\n", (int) hello.len, hello.data);


  str s = (str) { .len = 10, .data = "foo,barsky" };
  str foo, barsky;
  if(split(s, ',', &foo, &barsky)) {
    dbg("got: foo: %.*s, barsky: %.*s", (int) foo.len, foo.data, (int) barsky.len, barsky.data);
  }

  char *msg = "/data/666/420/hello world/";
  str *parts = split_all((str){.len=strlen(msg),.data=msg}, '/');
  dbg("got %ld parts", arrlen(parts));
  for(int i=0;i<arrlen(parts);i++) {
    str p = parts[i];
    dbg("part%d: %.*s", i, (int) p.len, p.data);
    }*/
}
