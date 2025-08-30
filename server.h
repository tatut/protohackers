#ifndef server_h
#define server_h
#include <errno.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdatomic.h>

/* thread workers = N worker threads are spawned which handle connections as they come
 * select = single server thread that uses select() to handle multiple clients
 * dgram = listen to UDP packets, invoke handler for each
 *
 * in select mode, each socket has associated state (alloc'ed by server)
 * that is passed in every time the handler is invoked for the socket.
 *
 * in dgram mode, the dgram_handler is used with a struct dgram param
 */
typedef enum { SERVER_THREAD_WORKERS=0, SERVER_SELECT=1, SERVER_DGRAM } server_type;

typedef struct dgram {
  int socket;
  uint8_t *data; // raw byte data (not nul terminated)
  uint16_t len; // length
  struct sockaddr_in *from;
} dgram;

typedef struct conn_state {
  int socket; // if 0, this is a free slot
  void* data;

  // internal, for broadcast
  struct conn_state *_conns;
} conn_state;

typedef struct server_worker_args {
  int server_socket;
  void *data;
  void (*handler)(conn_state*);
} server_worker_args;


typedef struct server {
  uint16_t port;
  int threads;
  int backlog;
  pthread_t *workers;
  atomic_bool shutdown;
  void (*handler)(conn_state*);
  void (*dgram_handler)(dgram*);
  void *data;
  server_type type;
  size_t connection_data_size;

  conn_state *conns;

} server;



#define serve(...) _serve((server) { __VA_ARGS__ })
void _serve(server s);

int readch(int socket);
bool read_until(int socket, char *to, char endch, size_t maxlen);

#define VA_ARGS(...) , ##__VA_ARGS__
#define err(fmt, ...) fprintf(stderr, "[ERROR] " fmt "\n" VA_ARGS(__VA_ARGS__))
#ifdef DEBUG
#define dbg(fmt, ...) fprintf(stdout, "[DEBUG] " fmt "\n" VA_ARGS(__VA_ARGS__))
#else
#define dbg(...)
#endif


#endif

#ifdef SERVER_IMPLEMENTATION

void _server_worker(server_worker_args *args) {
  struct sockaddr_in client_addr;
  int addr_len = sizeof(client_addr);

  while(1) {
    const int client_socket = accept(args->server_socket, (struct sockaddr *) &client_addr, (socklen_t *) &addr_len);
    if (client_socket < 0) {
      fprintf(stderr, "Worker thread accept failed, errno: %d", errno);
      sleep(1);
      continue;
    }
    //dbg("WORKER GOT CLIENT %d" , client_socket);
    conn_state cs = {.socket = client_socket, .data = args->data };
    //dbg("CALLING HANDLER");
    args->handler(&cs);
    //dbg("HANDLER DONE");
    close(client_socket);
    //dbg("CONN DONE");
  }
}

void _serve(server s) {
  if(s.threads == 0) s.threads = 10;
  if(s.backlog == 0) s.backlog = 10;
  if(s.port == 0) s.port = 8088;

  struct sockaddr_in server_addr;
  const int server_fd =
    socket(AF_INET, s.type == SERVER_DGRAM ? SOCK_DGRAM : SOCK_STREAM, 0);
  if(!server_fd) {
    fprintf(stderr, "Failed to create server socket.\n");
    return;
  }
  const int opt = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
    fprintf(stderr,"Failed to set socket options\n");
    return;
  }

  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(s.port);

  // Bind socket
  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    fprintf(stderr, "Bind failed to port: %d\n", s.port);
    return;
  }

  // Listen for connections
  if(s.type != SERVER_DGRAM) {
    if (listen(server_fd, s.backlog) < 0) {
      perror("Listen failed");
      return;
    }
  }

  if(s.type == SERVER_THREAD_WORKERS) {
    // Start workers
    s.shutdown = ATOMIC_VAR_INIT(false);
    s.workers = malloc(sizeof(pthread_t)*s.threads);
    server_worker_args args = {
      .handler = s.handler,
      .data = s.data,
      .server_socket = server_fd};
    for(int i=0; i<s.threads;i++) {
      pthread_create(&s.workers[i], NULL, (void*) _server_worker, &args);
    }
    // Wait for workers to be done
    for(int i=0; i < s.threads; i++) {
      pthread_join(s.workers[i], NULL);
    }
    free(s.workers);
    dbg("ALL THREADS DONE");
  } else if(s.type == SERVER_SELECT) {
    // Single threaded select server
    fd_set ready;
    #define MAX_CONNS 64
    conn_state conns[MAX_CONNS] = {0}; // PENDING: do we need more?
    struct sockaddr_in client_addr;
    int addr_len = sizeof(client_addr);

    while(1) {
      FD_ZERO(&ready);
      FD_SET(server_fd, &ready);
      int max_fd = server_fd;
      for(int i=0;i<MAX_CONNS;i++) {
        if(conns[i].socket > 0) {
          FD_SET(conns[i].socket, &ready);
          max_fd = conns[i].socket > max_fd ? conns[i].socket : max_fd;
        }
      }
      const int activity = select(max_fd+1, &ready, NULL, NULL, NULL);
      if(activity < 0 && errno != EINTR) {
        perror("Select error");
      }

      // handle new connection
      if(FD_ISSET(server_fd, &ready)) {
        // find empty slot
        int i=0;
        while(i < MAX_CONNS && conns[i].socket > 0) i++;
        if(i==MAX_CONNS) {
          err("Too many connections!");
        } else {
          conns[i].socket = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t*)&addr_len);
          dbg("got connection %d", conns[i].socket);
          if(s.connection_data_size) {
            if(conns[i].data) {
              // already allocated, zero it out
              dbg("zeroing old state");
              memset(conns[i].data, 0, s.connection_data_size);
            } else {
              dbg("new state");
              conns[i].data = calloc(1, s.connection_data_size);
            }
          }
          conns[i]._conns = conns;
          s.handler(&conns[i]);
        }
      }
      dbg("checking conns!");
      // check any connections for activity
      for(int i=0;i<MAX_CONNS;i++) {
        if(conns[i].socket > 0 && FD_ISSET(conns[i].socket, &ready)) {
          s.handler(&conns[i]);
        }
      }
    }
  } else if(s.type == SERVER_DGRAM) {
    while(1) {

      uint8_t buf[1024];
      struct sockaddr_in client_addr;
      socklen_t addr_len = sizeof(client_addr);
      int r = recvfrom(server_fd, buf, 1024, 0, (struct sockaddr *)&client_addr, &addr_len);
      if(r < 0) {
        err("UDP receive failed");
      }
      buf[r] = 0; // nul terminate
      dgram d = {.socket = server_fd, .data = buf, .len = r, .from = &client_addr};
      s.dgram_handler(&d);

    }

  } else {
    err("Unknown server type");
    exit(1);
  }

}

int readch(int socket) {
  char ch;
  if(read(socket, &ch, 1) != 1) {
    //perror("readch");
    return -1;
  }
  return ch;
}

bool read_until(int socket, char *to, char endch, size_t maxlen) {
  char ch;
  while(1) {
    if(maxlen == 0) return false;
    ch = readch(socket);
    if(ch == -1) return false;
    if(ch == endch) { *to = 0; return true; }
    *to++ = ch;
    maxlen--;
  }
  return false; // unreachable
}

#define each_conn_data(c, conn, type, item, body)                              \
  conn_state *__each_conns = (c)->_conns;                                      \
  for (int __each_i = 0; __each_i < MAX_CONNS; __each_i++) {                   \
    if (__each_conns[__each_i].socket != 0 &&                                  \
        __each_conns[__each_i].socket != (c)->socket) {                        \
      conn_state *conn = &__each_conns[__each_i];                              \
      type item = (type)__each_conns[__each_i].data;                           \
      body                                                                     \
    }                                                                          \
  }


#endif
