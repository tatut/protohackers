#include <string.h>
#define SERVER_IMPLEMENTATION
#include "server.h"

typedef enum participant_state {
  PS_INITIAL = 0,
  PS_HELLO_SENT = 1,
  PS_CHATTING = 2
} participant_state;

typedef struct participant {
  participant_state state;
  char name[17]; // 16 chars + nul
  char msg[512]; // buffer to read incoming message
  int read_pos; // position of reading to msg
} participant;

typedef struct chat {
  int num_participants;
  participant *participants;

} chat;

chat c = {0};

/* Read at most 512 byte line from client.
 * Returns true if line was read, false if not ready yet. */
bool participant_read_line(conn_state *c, char *to) {
  participant *p = (participant*)c->data;
  int r = recv(c->socket, &p->msg[p->read_pos], 512 - p->read_pos, MSG_DONTWAIT);
  if(r == 0) {
    dbg("Read 0, closing socket.");
    close(c->socket);
    c->socket = 0;
    return false;
  }
  p->read_pos += r;
  char *line_end = strchr(p->msg, '\n');
  if(line_end) {
    // got line end
    *line_end = 0;
    strcpy(to, p->msg);
    // FIXME: there might be data after the newline from client
    // should copy it, depends on how client flushes
    p->read_pos = 0;
    return true;
  }
  return false; // new newline encountered yet
}

/* broadcast to all other connections that are in CHATTING state*/
void broadcast(conn_state *from, uint8_t *data, size_t len) {
  each_conn_data(from, other_conn, participant*, pt, {
      if(pt->state == PS_CHATTING) {
        write(other_conn->socket, data, len);
      }
    });
}


void chat_handler(conn_state *c) {
  participant *p = (participant*)c->data;
  char buf[512], out[512];
  switch(p->state) {
  case PS_INITIAL: {
    // new connection
    write(c->socket, "Who dis?\n", 9);
    p->state = PS_HELLO_SENT;
    break;
  }
  case PS_HELLO_SENT:
    // reading the name
    if(participant_read_line(c, buf)) {
      char *n = buf;
      bool name_valid = true;
      if(*n == 0) name_valid = false;
      else {
        while(*n) {
          if(!((*n >= 'a' && *n <= 'z') ||
               (*n >= 'A' && *n <= 'Z') ||
               (*n >= '0' && *n <= '9'))) {
            name_valid = false;
            break;
          }
          n++;
        }
      }
      if(!name_valid) {
        dbg("Illegal name: %s", buf);
        close(c->socket);
        c->socket = 0;
        return;
      }

      strncpy(p->name, buf, 17);
      p->state = PS_CHATTING;
      // write who are the participants

      size_t write_pos = snprintf(buf, 512, "* Chatting: ");
      bool first = true;
      each_conn_data(c, _c, participant*, pt, {
          if(pt->state == PS_CHATTING) {
            if(write_pos >= 510) {
              err("Not enough space!");
              return; // FIXME: what to do?
            }
            write_pos += snprintf(&buf[write_pos], 512-write_pos, "%s%s",
                                  first ? "" : ", ", pt->name);
            first = false;
          }
        });
      write_pos += snprintf(&buf[write_pos], 512-write_pos, "\n");
      write(c->socket, buf, write_pos);

      // Send join notification to everyone chatting
      size_t len = snprintf(buf, 512, "* %s joined\n", p->name);
      broadcast(c, (uint8_t*)buf, len);
    }
    break;
  case PS_CHATTING:
    if(participant_read_line(c, buf)) {
      printf("chat BUF IS: \"%s\"\n", buf);
      // send line to everyone
      size_t len = snprintf(out, 512, "[%s] %s\n", p->name, buf);
      broadcast(c, (uint8_t*)out, len);
    } else if(c->socket == 0) {
      // we just closed this
      size_t len = snprintf(buf, 512, "* %s left\n", p->name);
      broadcast(c, (uint8_t*) buf, len);
    }

    break;
  }
}

int main(int argc, char **argv) {
  serve(.type = SERVER_SELECT,
        .connection_data_size = sizeof(participant),
        .handler=chat_handler);
}
