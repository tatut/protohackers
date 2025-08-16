#define SERVER_IMPLEMENTATION
#include "server.h"
#define JSON_IMPLEMENTATION
#include "json.h"

bool parse_request(char *data, char *method, double *number) {
  char **at = &data;
  bool has_method=false, has_number=false;
  json_object(at, {
      json_field_req("method", json_string_64, method, has_method)
      json_field_req("number", json_double, number, has_number)
      json_ignore_unknown_fields()
    })
  return has_method && has_number;
};

bool is_prime(double v) {
  dbg("double: %f", v);
  long n = (long) v;
  if(v - (double)n != 0.0) return false;
  if(n <= 1) return false;
  if(n <= 3) return true;
  if(n%2 == 0 || n%3 == 0) return false;
  for(long i=5; i*i <= n; i += 6) {
    if(n%i == 0 || n%(i+2) == 0) return false;
  }
  return true;
}


void prime_time(int socket, void *_data) {
  char *buf = malloc(65536);
  while(1) {
    // read until newline
    if(!read_until(socket, buf, '\n', 65536)) {
      err("failed to read line, closing socket %d", socket);
      goto done;
    }
    char method[64] = {0};
    double number;
    if(parse_request(buf, method, &number) && strcmp(method, "isPrime")==0) {
      char response[128];
      size_t len = snprintf(response, 128,
                            "{\"method\":\"isPrime\",\"prime\":%s}\n",
                            is_prime(number) ? "true" : "false");
      write(socket, response, len);
    } else {
      // malformed response
      write(socket, "FAIL\n", 5);
      break;
    }
  }
 done:
  free(buf);
}


int main(int argc, char **argv) {

  serve(.port = 8088, .handler = prime_time);
}
