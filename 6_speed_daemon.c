#include <math.h>
#include <stdint.h>
#include <string.h>
#define SERVER_IMPLEMENTATION
#include "server.h"
#define ARENA_IMPLEMENTATION
#define error err
#include "arena.h"

#define STB_DS_IMPLEMENTATION
#include "stb/stb_ds.h"

#include <sys/time.h>
#include <sys/ioctl.h>

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef size_t sz;
typedef ssize_t ssz;

bool read_str(arena *a, conn_state *c, char **to) {
  u8 len;
  if(recv(c->socket, &len, 1, MSG_WAITALL) < 1) {
    err("Could not read string length");
    return false;
  }
  char *str = malloc(len+1);//new(a, char, len+1);
  str[len] = 0;
  if(recv(c->socket, str, len, MSG_WAITALL) < len) {
    err("Could not read str of len %d", len);
    return false;
  }
  *to = str;
  return true;
}

bool read_u16(conn_state *c, u16 *to) {
  u16 num;
  ssz r;
  if((r=recv(c->socket, &num, 2, MSG_WAITALL)) < 2) {
    err("Couldn't read u16, read: %ld", r);
    return false;
  }
  *to = ntohs(num);
  return true;
}

bool read_u32(conn_state *c, u32 *to) {
  u32 num;
  if(recv(c->socket, &num, 4, MSG_WAITALL) < 4) {
    err("Couldn't read u32");
    return false;
  }
  *to = ntohl(num);
  return true;
}

bool read_u8(conn_state *c, u8 *to) {
  if(recv(c->socket, to, 1, MSG_WAITALL) < 1) {
    err("Couldn't read u8");
    return false;
  }
  return true;
}

/* current time in millis */
long now() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
}

typedef enum { UNKNOWN=0, CAMERA, DISPATCHER } ClientType;

typedef struct Camera {
  u16 road, mile, limit;
} Camera;

typedef struct Dispatcher {
  u8 numroads;
  u16 *roads;
  int socket; // for sending tickets
} Dispatcher;

typedef struct Client {
  ClientType type;
  union {
    Camera camera;
    Dispatcher dispatcher;
  } data;
} Client;

typedef struct CarPos {
  u32 ts;
  u16 road, mile, limit;
} CarPos;

typedef struct DayTicket {
  uint32_t key; // the day floot(ts/86400)
  bool value;
} DayTicket;

typedef struct Car {
  CarPos *observations;
  DayTicket *tickets;
} Car;

typedef struct CarMap {
  char *key;
  Car value;
} CarMap;

typedef struct Ticket {
  char *plate;
  u16 road;
  u16 mile1;
  u32 ts1;
  u16 mile2;
  u32 ts2;
  u16 speed;
} Ticket;


typedef struct SpeedDaemon {
  arena a;
  pthread_mutex_t lock; // mutation lock
  CarMap *car_table; // hashtable of cars (plate -> car)
  Dispatcher *dispatchers;
  Ticket *tickets; // pending tickets to send
} SpeedDaemon;

SpeedDaemon state = {0};

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
#define locking(state)                                                       \
  defer(mutex, lock(&(state)->lock), pthread_mutex_unlock(&(state)->lock))

// functions operating on state

void init_state(SpeedDaemon *state) { pthread_mutex_init(&state->lock, NULL); }

// sort by mile and road
int compare_road_and_ts(const void *a, const void *b) {
  CarPos *ca = (CarPos*) a, *cb = (CarPos*) b;
  //dbg("compare (r: %d, ts: %d) with (r: %d, ts: %d)", ca->road, ca->ts, cb->road, cb->ts);
  int rd = ca->road - cb->road;
  if(rd == 0) {
    return ca->ts - cb->ts;
  } else {
    return rd;
  }
}



void maybe_send_ticket(SpeedDaemon *state, Car *car, char *plate, CarPos a, CarPos b) {
  long speed = round(fabs(3600.0 * (b.mile-a.mile) / (b.ts-a.ts)));
  dbg("CHECK %s,  r: %d, m1: %d, ts1: %d,    m2: %d, ts2: %d,   speed %ld, limit %d", plate, a.road,
      a.mile, a.ts, b.mile, b.ts,
      speed, b.limit);
  uint32_t day1 = floor(a.ts/86400.0);
  uint32_t day2 = floor(b.ts/86400.0);
  if(speed > b.limit) {
    if(hmget(car->tickets, day1) || hmget(car->tickets, day2)) {
      dbg("  ALREADY SENT for ticket to %s for day: %d - %d", plate, day1, day2);
    } else {
      dbg("  SENDING TICKET to %s for day: %d - %d", plate, day1, day2);
      hmput(car->tickets, day1, true);
      hmput(car->tickets, day2, true);
      Ticket t = (Ticket) {
        .plate = plate,
        .road = a.road,
        .mile1 = a.mile,
        .ts1 = a.ts,
        .mile2 = b.mile,
        .ts2 = b.ts,
        .speed = speed
      };
      arrput(state->tickets, t);

      //send_ticket(state, car, a.road, a.mile, a.ts, b.mile, b.ts, speed);
    }


  }
}

void add_car_position(SpeedDaemon *state, char *plate, CarPos car_pos) {
  locking(state) {

    CarMap *cm = shgetp_null(state->car_table, plate);
    if(cm == NULL) {
      shput(state->car_table, plate, (Car){});
      cm = shgetp_null(state->car_table, plate);
    }
    assert(cm != NULL);
    Car *car = &cm->value;

    arrput(car->observations, car_pos);

    // debug
    dbg("Car with plate %s has following observations (%ld): ", plate, arrlen(car->observations));
    qsort(car->observations, arrlen(car->observations), sizeof(CarPos), compare_road_and_ts);
    for(int i=0;i<arrlen(car->observations);i++) {
      CarPos obs = car->observations[i];
      dbg(" at time %d :: road: %d, mile: %d, limit: %d", obs.ts, obs.road, obs.mile, obs.limit);
    }

    // Check for tickets
    // we need to check between any 2 observations on the same road
    int road = -1, i=0, start_idx = 0;
    for(i=0;i<arrlen(car->observations); i++) {
      CarPos p = car->observations[i];
      //dbg("  i: %d, road: %d, start_idx : %d", i, p.road, start_idx);
      if(p.road != road) {
        if(i-start_idx > 0) {
          dbg("checking tickets for road: %d", p.road);
          // have 2 or more observations, check for tickets
          for(int j=start_idx; j<i; j++) {
            for(int k=j+1; k<i; k++) {
              CarPos a = car->observations[j];
              CarPos b = car->observations[k];
              maybe_send_ticket(state, car, cm->key, a, b);
            }
          }
        }
        road = p.road;
        start_idx = i;
      }
    }

    // last batch
    if(start_idx < arrlen(car->observations)-1) {
      dbg("checking tickets for road: %d, start_idx=%d", car->observations[start_idx].road, start_idx);
      for(int j=start_idx; j<arrlen(car->observations); j++) {
        for(int k=j+1; k<arrlen(car->observations); k++) {
          dbg(" compare obs %d with %d", j,k);
          CarPos a = car->observations[j];
          CarPos b = car->observations[k];
          maybe_send_ticket(state, car, cm->key, a, b);
        }
      }
    }

  }
}

void add_dispatcher(SpeedDaemon *state, Dispatcher dispatcher) {
  locking(state) {
    arrput(state->dispatchers, dispatcher);
  }
}

void remove_dispatcher(SpeedDaemon *state, Dispatcher dispatcher) {
  locking(state) {
    for(int i=0;i<arrlen(state->dispatchers);i++) {
      if(state->dispatchers[i].socket == dispatcher.socket) {
        arrdelswap(state->dispatchers, i);
        break;
      }
    }
  }
}

#define READ(type, to)                                                         \
  do {                                                                         \
    if (!read_##type(c, &to)) {                                                \
      dbg("socket %d, read " #type " failed at line: %d", c->socket,           \
          __LINE__);                                                           \
      goto fail;                                                               \
    }                                                                          \
  } while (false)



// Ticket sender thread
void ticket_sender(SpeedDaemon *state) {
  // poll for new tickets
  while(1) {
    usleep(1000);
    locking(state) {
      int pending_tickets = arrlen(state->tickets);
      if(pending_tickets) {
        dbg("Pending tickets: %d", pending_tickets);
        Ticket t = state->tickets[0];

        Dispatcher to;
        bool found=false;
        dbg("Sending ticket, dispatchers: %ld", arrlen(state->dispatchers));
        for(int i=0; i<arrlen(state->dispatchers);i++) {
          Dispatcher d = state->dispatchers[i];
          for(int r=0;r<d.numroads;r++) {
            if(d.roads[r] == t.road) {
              dbg("Sending to dispatcher %d (socket %d, road %d)", i, d.socket, t.road);
              to = d;
              found = true;
            }
          }
        }
        if(found) {
          int len = strlen(t.plate);
          char head[2] = { 0x21, len };
          write(to.socket, &head, 2);
          write(to.socket, t.plate, len);
          uint16_t out = htons(t.road);
          write(to.socket, &out, 2);
          out = htons(t.mile1);
          write(to.socket, &out, 2);
          uint32_t out32 = htonl(t.ts1);
          write(to.socket, &out32, 4);
          out = htons(t.mile2);
          write(to.socket, &out, 2);
          out32 = htonl(t.ts2);
          write(to.socket, &out32, 4);
          out = htons(t.speed*100);
          write(to.socket, &out, 2);
          // remove this ticket, as we successfully sent it
          arrdelswap(state->tickets, 0);
        } else {
          // PENDING: do we need to try
          dbg("  couldn't send ticket yet, no dispatcher found!");
          // swap this to last place
          arrdel(state->tickets, 0);
          arrput(state->tickets, t);
        }
      }
    }
  }
}

void speed_daemon(conn_state *c) {
  Client client = {0};
  arena a = {0};
  int heartbeat = 0; // heartbeat in ms
  long last_heartbeat = 0;
  char *error; // send error to client

  while(1) {
    if(heartbeat > 0) {
      long n = now();
      if(n - last_heartbeat >= heartbeat) {
        //dbg("Sending heartbeat");
        u8 hb = 0x41;
        write(c->socket, &hb, 1);
        last_heartbeat = n;
      }
    }
    int available;
    if(ioctl(c->socket, FIONREAD, &available) < 0) perror("ioctl");
    if(!available) {
      usleep(10000);
      continue;
    }
    //dbg("SOCKET %d HAS %d bytes available", c->socket, available);
    // FIXME: use select with timeout of heartbeat
    u8 type;
    READ(u8, type);
    switch(type) {
    case 0x20: { // Plate
      if(client.type != CAMERA) {
        error = "not a camera";
        goto fail;
      }
      char *plate;
      u32 ts;
      if(!read_str(&a, c, &plate)) goto fail;
      READ(u32, ts);
      dbg("%d Plate: plate=%s, ts=%d", c->socket, plate, ts);
      add_car_position(&state, plate, (CarPos) {
          .ts = ts,
          .road = client.data.camera.road,
          .mile = client.data.camera.mile,
          .limit = client.data.camera.limit });
      break;
    }
    case 0x40: { // WantHeartBeat
      u32 interval;
      READ(u32, interval);
      dbg("%d WantHeartBeat: interval=%d", c->socket, interval);
      heartbeat = interval*100;
      last_heartbeat = now();
      break;
    }
    case 0x80: { // IAmCamera
      if(client.type != UNKNOWN) {
        err("Client identified as camera, but type was already set: %d", client.type);
        error = "already identified";
        goto fail;
      }
      client.type = CAMERA;

      READ(u16, client.data.camera.road);
      READ(u16, client.data.camera.mile);
      READ(u16, client.data.camera.limit);
      dbg("%d IAmCamera: road=%d, mile=%d, limit=%d", c->socket,
          client.data.camera.road,
          client.data.camera.mile,
          client.data.camera.limit);
      break;
    }
    case 0x81: { // IAmDispatcher
      if(client.type != UNKNOWN) {
        err("Client identified as dispatcher but type was already set: %d", client.type);
        error = "already identified";
        goto fail;
      }
      client.type = DISPATCHER;
      READ(u8, client.data.dispatcher.numroads);
      //dbg("  numroads: %d" ,client.data.dispatcher.numroads);
      client.data.dispatcher.roads = new(&a, u16, client.data.dispatcher.numroads);
      if(!client.data.dispatcher.roads) { err("couldn't alloc roads"); goto fail;}
      for(int i=0;i<client.data.dispatcher.numroads;i++) {
        //dbg("  socket %d reading road %d", c->socket, i);
        u16 road;
        READ(u16, road);
        //dbg("got road[%d] = %hu", i, road);
        client.data.dispatcher.roads[i] = road;
      }
      dbg("%d IAmDispatcher: numroads=%hu, ...", c->socket, client.data.dispatcher.numroads);
      client.data.dispatcher.socket = c->socket;
      add_dispatcher(&state, client.data.dispatcher);
      break;
    }
    default:
      dbg("Unkown message: %d", type);
      error = "Unrecognized message";
      goto fail;
    }
  }
 fail:
  if(error) {
    int len = strlen(error);
    dbg("%d sending error: %s (%d)", c->socket, error, len);
    uint8_t errbuf[256] = { 0x10, (uint8_t) len };

    memcpy(&errbuf[2], error, len);
    if(send(c->socket, errbuf, len+2, 0) < len+2) {
      perror("send");
    };
  }

  // remove dispatcher, if I am one
  if(client.type == DISPATCHER) {
    remove_dispatcher(&state, client.data.dispatcher);
  }
  dbg("client done");
}


int main(int argc, char **argv) {
  sh_new_strdup(state.car_table);

  pthread_mutex_init(&state.lock, NULL);

  pthread_t ticket_thread;
  pthread_create(&ticket_thread, NULL, (void*)ticket_sender, &state);

  signal(SIGPIPE, SIG_IGN);
  serve(.type=SERVER_THREAD_WORKERS, .threads = 200, .handler=speed_daemon);

}
