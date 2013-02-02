#!/bin/bash

CC=gcc
CCOPTS="-Wall"

$CC $CCOPTS paxos-server.c paxos.c net.c -o paxos-server
$CC $CCOPTS paxos-client.c paxos.c net.c -o paxos-client
