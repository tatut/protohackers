#define SERVER_IMPLEMENTATION
#include "server.h"

void echo(conn_state *c) {
  char buf[512];
  int r;
  printf("got socket: %d\n", c->socket);
  while((r = read(c->socket, buf, 512)) > 0) {
    write(c->socket, buf, r);
  }
}

int main(int argc, char **argv) {

  serve(.port = 8088, .handler = echo);

}
