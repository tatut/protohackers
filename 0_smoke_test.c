#define SERVER_IMPLEMENTATION
#include "server.h"

void echo(int socket, void *data) {
  char buf[512];
  int r;
  printf("got socket: %d\n", socket);
  while((r = read(socket, buf, 512)) > 0) {
    write(socket, buf, r);
  }
}

int main(int argc, char **argv) {

  serve(.port = 8088, .handler = echo);

}
