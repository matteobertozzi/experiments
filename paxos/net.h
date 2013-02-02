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

#ifndef _PAXOS_NET_H_
#define _PAXOS_NET_H_

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>

#include "paxos.h"

typedef struct udp_client {
  struct sockaddr_in addr;
  socklen_t addrlen;
} udp_client_t;

int udp_bind            (unsigned short port);
int udp_recv            (int sock,
                         udp_client_t *client,
                         paxos_message_t *message,
                         unsigned int msec);
int udp_send            (int sock,
                         const udp_client_t *client,
                         const paxos_message_t *message);
int udp_send_to         (const char *host,
                         unsigned int port,
                         const paxos_message_t *message);
int udp_broadcast       (const char *address,
                         unsigned int port,
                         const paxos_message_t *message);
int udp_client          (const char *host,
                         unsigned int port,
                         udp_client_t *client);
int udp_send_and_recv   (int sock,
                         udp_client_t *client,
                         paxos_message_t *message);

#endif

