#define SERVER_IMPLEMENTATION
#include "server.h"

#define JSON_IMPLEMENTATION
#include "json.h"

#define STB_DS_IMPLEMENTATION
#include "stb/stb_ds.h"

// 16k buffer for both reading and response,
// should fit the request and response, increase if not
#define BUF_SIZE 16384

typedef struct job {
  char *payload;
  long priority;
  long id;
} job;

typedef enum { UNKNOWN=0, GET, PUT, DELETE, ABORT } rq_type;

#define MAX_QUEUES 16

typedef struct rq_queues {
  int num_queues;
  char *queues[MAX_QUEUES];
} rq_queues;

typedef struct rq_get {
  rq_queues queues;
  bool wait;
} rq_get;

typedef struct rq_delete {
  long job_id;
} rq_delete;

typedef struct rq_put {
  char *queue;
  long priority;
  char *payload;
  size_t payload_size;
} rq_put;

typedef struct rq_abort {
  long job_id;
} rq_abort;

typedef struct rq {
  rq_type type;
  union {
    rq_get get;
    rq_delete delete;
    rq_put put;
    rq_abort abort;
  };

} rq;

typedef struct queue {
  job *jobs;
  pthread_mutex_t lock; // lock for any operation
  pthread_cond_t job_available; // for waiting on a job
} queue;

typedef struct queue_map {
  char *key;
  queue *value;
} queue_map;

typedef enum { JOB_UNKNOWN = 0, JOB_QUEUED, JOB_IN_PROGRESS, JOB_DONE } job_status;

typedef struct job_status_map {
  long key;
  job_status value;
} job_status_map;

pthread_mutexattr_t mutex_attr;
pthread_mutex_t queues_lock, next_id_lock, job_status_lock;
queue_map *queues;
job_status_map *job_status_by_id;

long next_id = 1;


void lock(pthread_mutex_t *l, int LINE) {
  int c = 0;
  //  dbg("[LINE:%d] locking %p", LINE, l);
  while(pthread_mutex_trylock(l) != 0) {
    usleep(100000);
    c++;
    if(c % 10 == 0)
      dbg("[LINE:%d] Thread waiting on lock %p for %f seconds", LINE, l, c/10.0);
  }
}
void unlock(pthread_mutex_t *l, int LINE) {
  //  dbg("[LINE:%d] unlocked %p", LINE, l);
  pthread_mutex_unlock(l);
}

#define defer(name,start,end) for(int __##name = (start,0); !__##name; (__##name += 1), end)
#define locking(lck)                                                       \
  defer(mutex, lock(&(lck),__LINE__), unlock(&(lck),__LINE__))

queue *get_queue(char *id) {
  if(!id) {
    err("Queue id can't be NULL!");
    return NULL;
  }
  dbg("id ptr: %p", id);
  dbg("get_queue(\"%s\")", id);
  queue *ret=NULL;
  locking(queues_lock) {
    int idx = shgeti(queues, id);
    if(idx == -1) {
      // create new
      dbg("Creating new queue: %s", id);
      ret = malloc(sizeof(queue));
      if(!ret) {
        err("Unable to allocate new queue: %s", id);
      } else {
        ret->jobs = NULL;

        //pthread_cond_init(&ret->job_available, NULL);
        pthread_mutex_init(&ret->lock, &mutex_attr);
        shput(queues, id, ret);
      }
    } else {
      ret = queues[idx].value;
    }
  }
  return ret;
}

long get_next_id() {
  long id;
  locking(next_id_lock) {
    id = next_id;
    next_id++;
  }
  return id;
}

int compare_job_by_priority(job *a, job *b) {
  return (int) (a->priority - b->priority);
}

job enqueue_job(queue *q, long priority, char *payload) {
  long id = get_next_id();
  job j = (job) {.payload=payload, .id=id, .priority=priority};
  dbg("enqueue id: %ld", id);
  locking(q->lock) {
    arrput(q->jobs, j);
    // PENDING: we could use a priority queue, but for now just quicksort
    qsort(q->jobs, arrlen(q->jobs), sizeof(job), (void*)compare_job_by_priority);
    dbg("job enqueued and sorted, len: %ld", arrlen(q->jobs));
  }
  return j;
}

bool queue_peek(queue *q, job *j) {
  bool ok = false;
  locking(q->lock) {
    int l = arrlen(q->jobs);
    if(l) {
      *j = q->jobs[l-1];
      ok = true;
    }
  }
  return ok;
}

bool queue_take(queue *q, job *to, long expected_id, long expected_priority) {
  bool ret = false;
  locking(q->lock) {
    dbg("queue_take locked!");
    if(arrlen(q->jobs)) {
      dbg("there are %ld jobs", arrlen(q->jobs));
      job j = arrlast(q->jobs);
      if(j.id == expected_id && j.priority == expected_priority) {
        *to = arrpop(q->jobs);
        ret = true;
        dbg("GOT THE JOB: id=%ld, queue jobs left: %ld", to->id, arrlen(q->jobs));
      }
    }
  }
  dbg("    unlocked");
  return ret;
}

bool dequeue_job(rq_queues requested_queues, bool wait, job *out, char **qname) {
  // get all queues
  int num_qs=requested_queues.num_queues;
  queue *qs[num_qs]; // stack allocation

  for(int i=0; i<requested_queues.num_queues; i++) {
    qs[i] = get_queue(requested_queues.queues[i]);
  }

  while(1) {
    // get highest priority job from any of the queues
    int queue_idx;
    job j = {.id = -1, .priority = -1 };

    for(int i=0;i<num_qs;i++) {
      // peek the highest job
      job candidate;
      if(queue_peek(qs[i], &candidate)) {
        if(candidate.priority > j.priority) {
          queue_idx = i;
          j = candidate;
        }
      }
    }

    if(j.id < 0) {
      //dbg("No job found, wait: %d", wait);
      // if not waiting, and no job found, return now
      if(!wait) {
        return false;
      }
      usleep(10000); // try again in 10ms (FIXME: have pthread cond for any jobs)
    } else {
      dbg("Job id=%ld found with priority %ld in queue %s, try to take it", j.id, j.priority, requested_queues.queues[queue_idx]);
      // job found, try to grab it
      if(queue_take(qs[queue_idx], out, j.id, j.priority)) {
        *qname = requested_queues.queues[queue_idx];
        return true;
      }
      // could not take it, try again
    }
  }
}

bool parse_queues(char **json, rq_queues *q) {
  q->num_queues = 0;
#define json_array_append(X) do { \
    if(q->num_queues == MAX_QUEUES) { err("Too many queues"); return false; } \
    q->queues[q->num_queues++] = (X);                                   \
  } while(false)

  json_array(json, char*, json_string_ptr);
  return true;
#undef json_array_append
}

/* Parse and validate the request, returns true if request is valid. */
bool parse_request(rq *r, char *data) {
  char **json = &data;
  char type[64] = {0};
  char *queue;
  rq_queues queues = {.num_queues = 0};
  long id = -1, priority = -1;
  bool wait = false;
  char *payload;
  memset(r, 0, sizeof(rq));

  json_object(json, {
      json_field("request", json_string_64, type);
      json_field("queues", parse_queues, &queues);
      json_field("queue", json_string_ptr, &queue);
      json_field("id", json_long, &id);
      json_field("pri", json_long, &priority);
      json_field("wait", json_bool, &wait);
      json_field("job", json_raw, &payload);
      json_ignore_unknown_fields();
    });
  if(strcmp(type, "get") == 0) {
    r->type = GET;
    if(queues.num_queues == 0) {
      err("Expected queues in GET request");
      return false;
    }
    r->get.queues = queues;
    r->get.wait = wait;
  } else if(strcmp(type, "put") == 0) {
    r->type = PUT;
    r->put.payload = payload;
    r->put.priority = priority;
    r->put.queue = queue;
    if(!payload) {
      err("No payload in PUT request");
      return false;
    }
    if(priority < 0) {
      err("No priority in PUT request");
      return false;
    }
    if(!queue) {
      err("No queue in PUT request");
      return false;
    }
    return true;
  } else if(strcmp(type, "delete") == 0) {
    r->type = DELETE;
    r->delete.job_id = id;
    if(id == -1) {
      err("No id in DELETE request");
      return false;
    }
  } else if(strcmp(type, "abort") == 0) {
    r->type = ABORT;
    r->abort.job_id = id;
    if(id == -1) {
      err("No id in ABORT request");
      return false;
    }
  } else {
    err("Unrecognized request type: %s", type);
    return false;
  }
  return true;
}

void print_request(rq *r) {
  switch(r->type) {
  case GET: printf("GET queues=[");
    for(int i=0;i<r->get.queues.num_queues; i++) {
      if(i>0) printf(", ");
      printf("\"%s\"", r->get.queues.queues[i]);
    }
    printf("], wait? %s\n", r->get.wait ? "true" : "false");
    break;
  case PUT:
    printf("PUT priority=%ld, payload=%s\n", r->put.priority, r->put.payload);
    break;
  case ABORT:
    printf("ABORT job_id=%ld\n", r->abort.job_id);
    break;
  case DELETE:
    printf("DELETE job_id=%ld\n", r->delete.job_id);
  default:
    printf("Unknown req type: %d", r->type);
  }
}

void set_job_status(long id, job_status status) {
  locking(job_status_lock) {
    hmput(job_status_by_id, id, status);
  }
}

job_status get_job_status(long id) {
  job_status ret;
  locking(job_status_lock) {
    ret = hmget(job_status_by_id, id);
  }
  return ret;
}

bool read_rq(conn_state *s, char *buf, rq *r) {
  size_t line_pos = 0;
  int ch;
  while((ch = readch(s->socket)) != -1) {
    if(line_pos == BUF_SIZE) {
      err("Too large input, over %d bytes!", BUF_SIZE);
      return false;
    }
    if(ch == '\n') {
      buf[line_pos] = 0;
      dbg("<< %s", buf);
      if(parse_request(r, buf)) {
        return true;
      } else {
        // got invalid request, try again
        err("Invalid request or handling error");
        char *response = "{\"status\":\"error\"}\n";
        send(s->socket, response, strlen(response), 0);
        line_pos = 0;
      }
    } else {
      buf[line_pos++] = ch;
    }
  }
  return false;
}

void job_centre(conn_state *s) {
#define respond(fmt, ...) { rlen = snprintf(response,BUF_SIZE,fmt "\n" VA_ARGS(__VA_ARGS__)); if(rlen > BUF_SIZE) { err("too much data"); return; } send(s->socket, response, rlen, 0); }

  struct timeval tv;
  tv.tv_sec = 60;
  tv.tv_usec = 0;
  setsockopt(s->socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(struct timeval));
  setsockopt(s->socket, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(struct timeval));

  char *buf = malloc(BUF_SIZE);
  char *response = malloc(BUF_SIZE);
  size_t rlen;
  if(!buf || !response) {
    err("You sir, are out of memory!");
    return;
  }
  rq r;
  job working_on = {.id = -1};
  char working_on_queue[64];

  while(read_rq(s, buf, &r)) {
    switch(r.type) {
    case PUT: {
      queue *q = get_queue(r.put.queue);
      if(!q) return; // should not happen
      job j = enqueue_job(q, r.put.priority, r.put.payload);
      respond("{\"status\":\"ok\",\"id\":%ld}", j.id);
      set_job_status(j.id, JOB_QUEUED);
      break;
    }
    case GET: {
      char *qname;
      job j;
      if(!dequeue_job(r.get.queues, r.get.wait, &j, &qname)) {
        // no job found
        respond("{\"status\":\"no-job\"}");
      } else {
        // found the job
        if(working_on.id > 0) {
          // free old job payload and mark it as done
          set_job_status(working_on.id, JOB_DONE);
          free(working_on.payload);
        }
        respond("{\"status\":\"ok\",\"id\":%ld,\"job\":%s,\"pri\":%ld,\"queue\":\"%s\"}", j.id, j.payload, j.priority, qname);
        set_job_status(j.id, JOB_IN_PROGRESS);
        working_on = j;
        strncpy(working_on_queue, qname, 64);
      }
      break;
    }
    case ABORT: {
      if(r.abort.job_id != working_on.id ||
         get_job_status(r.abort.job_id) == JOB_DONE) {
        respond("{\"status\":\"no-job\"}");
      } else {
        enqueue_job(get_queue(working_on_queue), working_on.priority, working_on.payload);
        working_on.id = -1;
        respond("{\"status\":\"ok\"}");
      }
      break;
    }
    case DELETE: {
      job_status status = get_job_status(r.delete.job_id);
      if(status == JOB_IN_PROGRESS || status == JOB_QUEUED) {
        set_job_status(r.delete.job_id, JOB_DONE);
        // we need to find in which queue this job is in and delete it
        // FIXME: the status could have the queue_idx
        if(working_on.id == r.delete.job_id) {
          working_on.id = -1;
          free(working_on.payload);
        } else {
          locking(queues_lock) {
            for(int i=0;i<shlen(queues); i++) {
              queue_map *qm = &queues[i];
              for(int j=0;j<arrlen(qm->value->jobs); j++) {
                if(qm->value->jobs[j].id == r.delete.job_id) {
                  dbg("Found %dth job with id %ld in queue %s to delete", j, qm->value->jobs[j].id, qm->key);
                  arrdel(qm->value->jobs, j);
                  goto done;
                }
              }
            }
          done:
            dbg("delete done");
          }
        }
        respond("{\"status\":\"ok\"}");
      } else {
        respond("{\"status\":\"no-job\"}");
      }
      break;
    }

    case UNKNOWN:
      err("Unknown message, should be unreachable!");
      break;
    }

  }

  if(working_on.id != -1) {
    // abort any job we were working on
    dbg("implicit abort on client disconnect, q: %s, pri: %ld, payload: %s", working_on_queue, working_on.priority, working_on.payload);
    enqueue_job(get_queue(working_on_queue), working_on.priority, working_on.payload);
  }

  free(buf);
  free(response);
}


int main(int argc, char **argv) {
  pthread_mutexattr_init(&mutex_attr);
  pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_ERRORCHECK);

  pthread_mutex_init(&queues_lock, NULL);
  pthread_mutex_init(&next_id_lock, NULL);
  pthread_mutex_init(&job_status_lock, NULL);
  sh_new_strdup(queues);

  hmdefault(job_status_by_id, JOB_UNKNOWN);


  // ignore lost write errors to lost connections
  signal(SIGPIPE, SIG_IGN);

  serve(.backlog=1000, .threads=1100, .handler = job_centre);
}
