#include <math.h>
#include <stdint.h>
#include <string.h>
#define SERVER_IMPLEMENTATION
#include "server.h"
#define ARENA_IMPLEMENTATION
#define error err
#include "arena.h"
#define HASH_IMPLEMENTATION
#include "hash.h"
#define DYNARR_IMPLEMENTATION
#define panic(...) { err(__VA_ARGS__); exit(1); }
#include "dynarr.h"

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

DefineArray(CarPosArray, CarPos);

typedef struct Car {
  char *plate;
  CarPosArray observations;
} Car;

// Global state
#define HT_SIZE 4096

DefineArray(CarArray, Car);
DefineArray(DispatcherArray, Dispatcher);


typedef struct SpeedDaemon {
  arena a;
  pthread_mutex_t lock; // mutation lock
  CarArray bucket[HT_SIZE]; // hash buckets for cars
  DispatcherArray dispatchers;
} SpeedDaemon;

SpeedDaemon state = {0};


#define defer(name,start,end) for(int __##name = (start,0); !__##name; (__##name += 1), end)
#define locking(state)                                                       \
  defer(mutex, pthread_mutex_lock(&(state)->lock), pthread_mutex_unlock(&(state)->lock))

// functions operating on state

void init_state(SpeedDaemon *state) { pthread_mutex_init(&state->lock, NULL); }

// sort by mile and road
int compare_road_and_ts(const void *a, const void *b) {
  CarPos *ca = (CarPos*) a, *cb = (CarPos*) b;
  dbg("compare (r: %d, ts: %d) with (r: %d, ts: %d)", ca->road, ca->ts, cb->road, cb->ts);
  int rd = ca->road - cb->road;
  if(rd == 0) {
    return ca->ts - cb->ts;
  } else {
    return rd;
  }
}

void send_ticket(SpeedDaemon *state, char *plate, uint16_t road,
                 uint16_t mile1, uint32_t ts1,
                 uint16_t mile2, uint32_t ts2,
                 uint16_t speed) {
  Dispatcher to;
  bool found=false;
  da_foreach(Dispatcher, disp, &state->dispatchers, {
      for(int i=0;i<disp.numroads;i++) {
        if(disp.roads[i] == road) {
          dbg("Sending to dispatcher %d (socket %d, road %d)", i, disp.socket, road);
          to = disp;
          found = true;
          break;
        }
      }
    });
  if(found) {
    int len = strlen(plate);
    char head[2] = { 0x21, len };
    write(to.socket, &head, 2);
    write(to.socket, plate, len);
    uint16_t out = htons(road);
    write(to.socket, &out, 2);
    out = htons(mile1);
    write(to.socket, &out, 2);
    uint32_t out32 = htonl(ts1);
    write(to.socket, &out32, 4);
    out = htons(mile2);
    write(to.socket, &out, 2);
    out32 = htonl(ts2);
    write(to.socket, &out32, 4);
    out = htons(speed*100);
    write(to.socket, &out, 2);
  }

}

void add_car_position(SpeedDaemon *state, char *plate, CarPos car_pos) {
  locking(state) {
    unsigned long h = hash((uint8_t*)plate, strlen(plate));
    CarArray *bucket = &state->bucket[h%HT_SIZE];
    Car *car;
    for(int i=0; i<bucket->size; i++) {
      dbg(" bucket i=%d, plate: %s, compare to: %s", i, bucket->items[i].plate, plate);
      if(strcmp(bucket->items[i].plate, plate)==0) {
        car = &bucket->items[i];
        goto found;
      }
    }
    da_append(bucket, ((Car) {.plate = arena_str(&state->a, plate),
                              .observations={0}}));
    car = da_lastptr(Car, bucket);
  found:
    da_append(&car->observations, car_pos);
    // debug
    dbg("Car with plate %s has following observations (%d): ", plate, car->observations.size);
    qsort(car->observations.items, car->observations.size, sizeof(CarPos), compare_road_and_ts);
    da_foreach(CarPos, obs, &car->observations, {
        dbg(" at time %d :: road: %d, mile: %d, limit: %d", obs.ts, obs.road, obs.mile, obs.limit);
      });

    // Check for tickets
    int road = -1, mile = -1;
    long ts = -1;
    da_foreach(CarPos, p, &car->observations, {
        // consecutive observations on the same road, check speed
        if(road == p.road) {
          long speed = round(3600.0 * abs(p.mile-mile) / (p.ts-ts));
          dbg("  road: %d, mile: %d ==> SPEED: %ld %s", road, p.mile, speed, speed > p.limit ? "TICKET!" : "");
          // send ticket to dispatcher
          send_ticket(state, plate, road, mile, ts, p.mile, p.ts, speed);
        }
        road = p.road;
        mile = p.mile;
        ts = p.ts;

      });
  }
}

void add_dispatcher(SpeedDaemon *state, Dispatcher dispatcher) {
  da_append(&state->dispatchers, dispatcher);
}

#define READ(type, to)                                                         \
  do {                                                                         \
    if (!read_##type(c, &to)) {                                                \
      dbg("socket %d, read " #type " failed at line: %d", c->socket,           \
          __LINE__);                                                           \
      goto fail;                                                               \
    }                                                                          \
  } while (false)





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
        error = "No changing roles!";
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
        error = "No changing roles!";
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
    uint8_t len = strlen(error);
    char head[2] = { 0x10, len };
    write(c->socket, &head, 2);
    write(c->socket, error, len);
  }

  // remove dispatcher, if I am one

  dbg("client done");
}


int main(int argc, char **argv) {
  signal(SIGPIPE, SIG_IGN);
  serve(.type=SERVER_THREAD_WORKERS, .threads = 200, .handler=speed_daemon);


  add_car_position(&state, "FOOBAR", (CarPos) {.road=1234, .mile=8398, .limit=100, .ts=64823});
  add_car_position(&state, "FOOBAR", (CarPos) {.road=1234, .mile=8388, .limit=100, .ts=64523});

}
