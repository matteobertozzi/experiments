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

#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

#include "paxos.h"
#include "net.h"

static int __is_running = 1;
static void __signal_handler (int signum) {
    __is_running = 0;
}

#define NPENDING_CLIENTS     16
struct server {
  udp_client_t clients[NPENDING_CLIENTS];
  unsigned int num_clients;
  uint64_t num_broadcast;
  uint64_t num_send;
  paxos_t paxos;
  int sock;
};

static void __send_learned_value (struct server *server, const udp_client_t *client) {
  paxos_message_t message;
  message.paxos_id = server->paxos.learner.paxos_id;
  message.value = server->paxos.learner.learned_value;
  udp_send(server->sock, client, &message);
}

static void __wait_proposed (struct server *server, const udp_client_t *client) {
  /* Silent drop notification if we've too many pending requests */
  if (server->num_clients >= NPENDING_CLIENTS)
    return;

  /* Add client to the pending queue */
  memcpy(&(server->clients[server->num_clients++]), client, sizeof(udp_client_t));
}

static void __send_proposed (struct server *server, const udp_client_t *client) {
  if (server->paxos.learner.has_learned_value) {
    __send_learned_value(server, client);
  } else {
    __wait_proposed(server, client);
  }
}

static void __paxos_send (void *arg, uint64_t node_id, const paxos_message_t *message) {
  struct server *server = (struct server *)arg;
  fprintf(stderr, "send: to %lu message %u:%s node %lu\n",
          node_id, message->type, paxos_message_to_string(message), message->node_id);
  udp_send_to("127.0.0.1", (unsigned int)(8080 + (node_id & 0xffff)), message);
  server->num_send++;
}

static void __paxos_broadcast (void *arg, const paxos_message_t *message) {
  struct server *server = (struct server *)arg;
  int i;
  fprintf(stderr, "bcst: message %u:%s\n", message->type, paxos_message_to_string(message));
  for (i = 0; i < 10; ++i) {
    udp_broadcast("127.255.255.255", 8080 + i, message);
  }
  server->num_broadcast++;
}

static void __paxos_learned_value (void *arg) {
  struct server *server = (struct server *)arg;
  fprintf(stderr, "Hey paxos told me a new value! paxos_id: %lu value: %lu\n",
                  server->paxos.learner.paxos_id, server->paxos.learner.learned_value);

  while (server->num_clients > 0) {
    udp_client_t *client = &(server->clients[--(server->num_clients)]);
    __send_learned_value(server, client);
  }
}

int main (int argc, char **argv) {
  paxos_timeout_t *timeout;
  paxos_message_t message;
  paxos_context_t context;
  struct server server;
  udp_client_t client;

  /* Initialize signals */
  signal(SIGINT, __signal_handler);

  /* Initialize paxos context */
  context.send = __paxos_send;
  context.broadcast = __paxos_broadcast;
  context.learned_value = __paxos_learned_value;
  context.arg = &server;

  /* Initialize server */
  memset(&server, 0, sizeof(struct server));

  /* Initialize paxos */
  paxos_open(&(server.paxos), &context, argc, 3);

  fprintf(stderr, "PAXOS %lu MESSAGE %lu -> NODE ID: %lu -> PORT %lu\n",
    sizeof(paxos_t), sizeof(paxos_message_t),
    server.paxos.node_id, 8080 + server.paxos.node_id);

  /* Initialize UDP Server */
  if ((server.sock = udp_bind(8080 + server.paxos.node_id)) < 0) {
    perror("udp_bind()");
    return(1);
  }

  /* Bootstrap paxos */
  paxos_bootstrap(&(server.paxos));

  /* Start spinning... */
  while (__is_running) {
    timeout = paxos_timeout(&(server.paxos));
    if (udp_recv(server.sock, &client, &message, paxos_timeout_remaining(timeout)) < 0) {
      paxos_timeout_trigger(timeout);
    } else {
      printf("recv: %s:%d -> %u:%s from %lu (send: %lu broadcast: %lu)\n",
             inet_ntoa(client.addr.sin_addr), ntohs(client.addr.sin_port),
             message.type, paxos_message_to_string(&message), message.node_id,
             server.num_send, server.num_broadcast);

      switch (message.type) {
        case PAXOS_USER_PROPOSE_VALUE:
          fprintf(stderr, "USER PROPOSE VALUE %lu\n", message.value);
          paxos_propose(&(server.paxos), message.value);
          __wait_proposed(&server, &client);
          break;
        case PAXOS_USER_LEARN_VALUE:
          fprintf(stderr, "USER LEARN VALUE\n");
          __send_proposed(&server, &client);
          break;
        default:
          paxos_process_message(&(server.paxos), &message);
          break;
      }
    }
  }

  /* ...and we're done */
  paxos_close(&(server.paxos));
  close(server.sock);
  return(0);
}

