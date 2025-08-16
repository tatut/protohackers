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

typedef struct server_worker_args {
  int server_socket;
  void *data;
  void (*handler)(int,void*);
} server_worker_args;

typedef struct server {
  uint16_t port;
  int threads;
  int backlog;
  pthread_t *workers;
  atomic_bool shutdown;
  void (*handler)(int,void*);
  void *data;
} server;

#define serve(...) _serve((server) { __VA_ARGS__ })
void _serve(server s);

int readch(int socket);
bool read_until(int socket, char *to, char endch, size_t maxlen);

#define VA_ARGS(...) , ##__VA_ARGS__
#define err(fmt, ...) fprintf(stderr, "[ERROR] " fmt "\n" VA_ARGS(__VA_ARGS__))
#define dbg(fmt, ...) fprintf(stdout, "[DEBUG] " fmt "\n" VA_ARGS(__VA_ARGS__))

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
    args->handler(client_socket, args->data);
    close(client_socket);
  }
}

void _serve(server s) {
  if(s.threads == 0) s.threads = 10;
  if(s.backlog == 0) s.backlog = 10;
  if(s.port == 0) s.port = 8088;

  struct sockaddr_in server_addr;
  const int server_fd = socket(AF_INET, SOCK_STREAM, 0);
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
  if (listen(server_fd, s.backlog) < 0) {
    fprintf(stderr, "Listen failed\n");
    return;
  }
  // Start workers
  s.shutdown = ATOMIC_VAR_INIT(false);
  s.workers = alloca(sizeof(pthread_t)*s.threads);
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

}

int readch(int socket) {
  char ch;
  if(read(socket, &ch, 1) != 1) return -1;
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

#endif
