#define SERVER_IMPLEMENTATION
#include "server.h"
#include <netdb.h>
#include <ctype.h>

#define TONY_BOGUSCOIN_ADDR "7YWHMfk9JZe0LM0g1ZauHuiSxhI"

int rewrite_boguscoin(char *msg, int size, char *to) {
  char *start = to;
  size_t i=0;
  while(i < size && msg[i] != '\n') {
    switch(msg[i]) {
    case '7':
      // possible boguscoin address
      if(i == 0 || msg[i-1] == ' ') {
        int j=i+1;
        while(j < size-1 && isalnum(msg[j])) j++;
        int len = j-i;
        //dbg(" j: %d, size: %d,  next ch: '%c', len: %d", j, size, msg[j+1], len);
        if((msg[j] == ' ' || msg[j] == '\n') && len >= 26 && len <= 35) {
          strcpy(to, TONY_BOGUSCOIN_ADDR);
          to += 27;
          i = j-1;
          break;
        }
      }
      // fall through to default
    default:
      *to++ = msg[i];
    }
    i++;
  }
  if(msg[i] != '\n') return 0; // line not ready yet
  *to++ = '\n';
  return to-start;
}

void boguscoin_steal(conn_state *c) {
  struct sockaddr_in to = {.sin_family = AF_INET, .sin_port = htons(16963)};
  struct hostent *h = gethostbyname("chat.protohackers.com");
  if(h && h->h_addrtype == AF_INET) {
    to.sin_addr = *((struct in_addr **)h->h_addr_list)[0];
  } else {
    err("Can't resolve: chat.protohackers.com");
    return;
  }

  int upstream = socket(AF_INET, SOCK_STREAM, 0);
  if(upstream < 0) {
    err("Can't create socket");
    return;
  }
  struct timeval tv;
  tv.tv_sec = 60;
  tv.tv_usec = 0;
  setsockopt(upstream, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(struct timeval));
  setsockopt(upstream, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(struct timeval));

  if(connect(upstream, (struct sockaddr *)&to, sizeof(to)) < 0) {
    err("Connect to upstream failed");
    return;
  }

  bool client_first = true; // don't rewrite the name
  fd_set ready;
  #define SIZE 2048
  char client_buf[SIZE], upstream_buf[SIZE], steal[SIZE];
  int client_pos = 0, upstream_pos = 0;

  while(1) {
    FD_ZERO(&ready);
    FD_SET(c->socket, &ready);
    FD_SET(upstream, &ready);
    int act = select(1+(c->socket > upstream ? c->socket : upstream), &ready,
                     NULL, NULL, NULL);
    if(act < 0 && errno != EINTR) {
      perror("Select error");
    }
    dbg("----");
    if(FD_ISSET(c->socket, &ready)) {
      // message from client
      int r = recv(c->socket, &client_buf[client_pos], SIZE-client_pos, MSG_DONTWAIT);
      if(r == 0) goto end;
      else if(r < 0 && errno != EAGAIN) perror("client read failed");
      if(r > 0) {
        client_pos += r;
        client_buf[client_pos] = 0;
        r = rewrite_boguscoin(client_buf, client_pos, steal);
        if(r) {
          write(upstream, steal, r);
          client_pos = 0;
        }
      }
    }
    if(FD_ISSET(upstream, &ready)) {
      int r = recv(upstream, &upstream_buf[upstream_pos], SIZE-client_pos, MSG_DONTWAIT);
      if(r == 0) goto end;
      else if(r < 0 && errno != EAGAIN) perror("upstream read failed");
      if(r > 0) {
        upstream_pos += r;
        upstream_buf[upstream_pos] = 0;
        r = rewrite_boguscoin(upstream_buf, upstream_pos, steal);
        if(r) {
          write(c->socket, steal, r);
          upstream_pos = 0;
        }
      }
    }

  }
 end:
  dbg("closing, client: %d, upstream: %d", c->socket, upstream);
  close(upstream);

}
int main(int argc, char **argv) {
  serve(.handler = boguscoin_steal);
  /*
  char steal[SIZE];
  char *msg1 = "Hi alice, please send payment to 7iKDZEwPZSqIvDnHvVN2r0hUWXD5rHX\n";
  const size_t len1 = strlen(msg1);

  char *msg2 = "My addr is 7iKDZEwPZSqfoobarVN2r0hUWXD5rHX please help!\n";
  const size_t len2 = strlen(msg2);

  size_t l = rewrite_boguscoin(msg1, len1, steal);
  dbg("\nFROM: %s  TO: %.*s", msg1, (int)l, steal);

  l = rewrite_boguscoin(msg2, len2, steal);
  dbg("\nFROM: %s  TO: %.*s", msg2, (int)l, steal);
  */
}
