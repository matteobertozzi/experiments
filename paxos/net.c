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

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <netdb.h>

#include "paxos.h"
#include "net.h"

int udp_bind (unsigned short port) {
  struct sockaddr_in addr;
  int sock;
  int yep;

  if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    return(-1);

  yep = 1;
  setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yep, sizeof(int));

  memset(&addr, 0, sizeof(struct sockaddr_in));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);

  if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    return(-2);

  return(sock);
}

int udp_recv (int sock,
              udp_client_t *client,
              paxos_message_t *message,
              unsigned int msec)
{
  struct timeval tv;
  fd_set rfds;

  tv.tv_sec = (msec / 1000);
  tv.tv_usec = (msec % 1000) * 1000;

  FD_ZERO(&rfds);
  FD_SET(sock, &rfds);

  if (msec > 0) {
    if (select(sock + 1, &rfds, NULL, NULL, &tv) <= 0)
      return(-1);
  }

  client->addrlen = sizeof(struct sockaddr_in);
  return(recvfrom(sock, message, sizeof(paxos_message_t), 0,
                  (struct sockaddr *)&(client->addr), &(client->addrlen)));
}

int udp_send (int sock,
              const udp_client_t *client,
              const paxos_message_t *message)
{
  return(sendto(sock, message, sizeof(paxos_message_t), 0,
                (struct sockaddr *)&(client->addr), client->addrlen));
}

int udp_send_to (const char *host,
                 unsigned int port,
                 const paxos_message_t *message)
{
  udp_client_t client;
  int sock;
  int ret;

  if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    return(-2);

  memset(&(client.addr), 0, sizeof(struct sockaddr_in));
  client.addr.sin_family = AF_INET;
  client.addr.sin_addr.s_addr = inet_addr(host);
  client.addr.sin_port = htons(port);

  client.addrlen = sizeof(struct sockaddr_in);
  ret = udp_send(sock, &client, message);

  close(sock);
  return(ret != sizeof(paxos_message_t));
}

int udp_broadcast (const char *address,
                   unsigned int port,
                   const paxos_message_t *message)
{
  udp_client_t client;
  int sock;
  int ret;

  if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    return(-1);

  ret = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &ret, sizeof(int)) < 0) {
    close(sock);
    return(-1);
  }

  memset(&(client.addr), 0, sizeof(struct sockaddr_in));
  client.addr.sin_family = AF_INET;
  client.addr.sin_addr.s_addr = inet_addr(address);
  //client.addr.sin_addr.s_addr = inet_addr("127.255.255.255");
  //addr.sin_addr.s_addr = inet_addr("255.255.255.255");
  client.addr.sin_port = htons(port);

  client.addrlen = sizeof(struct sockaddr_in);
  ret = udp_send(sock, &client, message);

  close(sock);
  return(ret != sizeof(paxos_message_t));
}

int udp_client (const char *host,
                unsigned int port,
                udp_client_t *client)
{
  int sock;

  if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    return(-1);

  memset(&(client->addr), 0, sizeof(struct sockaddr_in));
  client->addr.sin_family = AF_INET;
  client->addr.sin_addr.s_addr = inet_addr(host);
  client->addr.sin_port = htons(port);

  return(sock);
}

int udp_send_and_recv (int sock,
                       udp_client_t *client,
                       paxos_message_t *message)
{
  /* send the message to the server */
  client->addrlen = sizeof(struct sockaddr_in);
  if (udp_send(sock, client, message) < 0) {
    perror("sendto()");
    return(-1);
  }

  /* wait the server response */
  if (udp_recv(sock, client, message, 0) != sizeof(paxos_message_t)) {
    perror("recvfrom()");
    return(-2);
  }

  return(0);
}

