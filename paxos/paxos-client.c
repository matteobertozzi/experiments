/*
 *   Copyright 2013 Matteo Bertozzi
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

#include "paxos.h"
#include "net.h"

static int __paxos_get (const char *host, unsigned int port) {
  paxos_message_t message;
  udp_client_t client;
  int sock;

  memset(&message, 0, sizeof(paxos_message_t));
  message.type = PAXOS_USER_LEARN_VALUE;

  if ((sock = udp_client(host, port, &client)) < 0)
    return(1);

  if (udp_send_and_recv(sock, &client, &message))
    return(1);

  printf("paxos_id: %lu value: %lu\n", message.paxos_id, message.value);
  close(sock);
  return(0);
}

static int __paxos_set (const char *host, unsigned int port, uint64_t value) {
  paxos_message_t message;
  udp_client_t client;
  int sock;

  memset(&message, 0, sizeof(paxos_message_t));
  message.type = PAXOS_USER_PROPOSE_VALUE;
  message.value = value;

  if ((sock = udp_client(host, port, &client)) < 0)
    return(1);

  if (udp_send_and_recv(sock, &client, &message))
    return(1);

  printf("paxos_id: %lu value: %lu\n", message.paxos_id, message.value);
  close(sock);
  return(0);
}

int main (int argc, char **argv) {
  unsigned int port;

  if (argc < 4 ||
     (!strncmp(argv[3], "get", 3) && argc > 4) ||
     (!strncmp(argv[3], "set", 3) && argc < 5))
  {
    fprintf(stderr, "usage:\n");
    fprintf(stderr, "  paxos-client <host> <port> get\n");
    fprintf(stderr, "  paxos-client <host> <port> set <value>\n");
    return(1);
  }

  port = strtoul(argv[2], NULL, 10) & 0xffff;
  if (!strncmp(argv[3], "get", 3))
    return(__paxos_get(argv[1], port));

  if (!strncmp(argv[3], "set", 3)) {
    uint64_t value = strtoul(argv[4], NULL, 10);
    return(__paxos_set(argv[1], port, value));
  }

  return(1);
}

