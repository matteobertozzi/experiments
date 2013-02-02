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

#ifndef _PAXOS_H_
#define _PAXOS_H_

typedef struct paxos_proposer_state paxos_proposer_state_t;
typedef struct paxos_acceptor_state paxos_acceptor_state_t;
typedef struct paxos_acceptor paxos_acceptor_t;
typedef struct paxos_proposer paxos_proposer_t;
typedef struct paxos_learner paxos_learner_t;
typedef struct paxos_context paxos_context_t;
typedef struct paxos_message paxos_message_t;
typedef struct paxos_timeout paxos_timeout_t;
typedef struct paxos_quorum paxos_quorum_t;
typedef struct paxos paxos_t;

typedef void (*paxos_callback_t)  (void *arg);
typedef void (*paxos_send_t)      (void *arg,
                                   uint64_t node_id,
                                   const paxos_message_t *message);
typedef void (*paxos_broadcast_t) (void *arg,
                                   const paxos_message_t *message);

enum paxos_message_type {
  /* Paxos */
  PAXOS_PREPARE_REQUEST             =  1,
  PAXOS_PREPARE_REJECTED            =  2,
  PAXOS_PREPARE_PREVIOUSLY_ACCEPTED =  3,
  PAXOS_PREPARE_CURRENTLY_OPEN      =  4,
  PAXOS_PROPOSE_REQUEST             =  5,
  PAXOS_PROPOSE_REJECTED            =  6,
  PAXOS_PROPOSE_ACCEPTED            =  7,
  PAXOS_LEARN_PROPOSAL              =  8,
  PAXOS_LEARN_VALUE                 =  9,
  PAXOS_REQUEST_CHOSEN              = 10,
  /* System */
  PAXOS_BOOTSTRAP                   = 21,
  PAXOS_CATCHUP_START               = 22,
  PAXOS_CATCHUP_REQUEST             = 23,
  PAXOS_CATCHUP_RESPONSE            = 24,
  /* User */
  PAXOS_USER_PROPOSE_VALUE          = 31,
  PAXOS_USER_LEARN_VALUE            = 32,
};

struct paxos_proposer_state {
  uint64_t proposal_id;
  uint64_t highest_received_proposal_id;
  uint64_t highest_promised_proposal_id;
  uint64_t proposed_value;
  uint8_t  preparing;
  uint8_t  proposing;
  uint8_t  learn_sent;
};

struct paxos_acceptor_state {
  uint64_t promised_proposal_id;
  uint64_t accepted_proposal_id;
  uint64_t accepted_value;
  uint8_t  accepted;
};

struct paxos_timeout {
  void *arg;
  paxos_callback_t callback;
  uint8_t active;
  uint32_t timeout;
  uint64_t expire_time;
};

struct paxos_message {
    uint8_t  type;
    uint8_t  __pad[3];
    uint64_t paxos_id;
    uint64_t node_id;
    uint64_t proposal_id;
    uint64_t accepted_proposal_id;
    uint64_t promised_proposal_id;
    uint64_t value;
};

struct paxos_acceptor {
  paxos_acceptor_state_t state;
  uint64_t        sender_id;
  uint64_t        written_paxos_id;
  uint8_t         is_committing;
};

struct paxos_proposer {
  paxos_proposer_state_t state;
  paxos_timeout_t prepare_timeout;
  paxos_timeout_t propose_timeout;
  paxos_timeout_t restart_timeout;
};

struct paxos_learner {
  uint64_t paxos_id;
  uint64_t learned_value;             /* TODO: store more than one value */
  uint8_t  has_learned_value;
  uint64_t last_request_chosen_time;
};

struct paxos_context {
  paxos_send_t send;
  paxos_broadcast_t broadcast;
  paxos_callback_t learned_value;
  void *arg;
};

struct paxos_quorum {
  uint16_t num_accepted;
  uint16_t num_rejected;
  uint32_t num_nodes;
};

struct paxos {
  paxos_context_t *context;
  paxos_proposer_t proposer;
  paxos_acceptor_t acceptor;
  paxos_learner_t  learner;
  paxos_quorum_t   quorum;
  uint64_t node_id;
};

const char *      paxos_message_to_string   (const paxos_message_t *message);

unsigned int      paxos_timeout_remaining   (paxos_timeout_t *self);
void              paxos_timeout_trigger     (paxos_timeout_t *self);

void              paxos_open                (paxos_t *self,
                                             paxos_context_t *context,
                                             uint64_t node_id,
                                             uint64_t num_nodes);
void              paxos_close               (paxos_t *paxos);
void              paxos_bootstrap           (paxos_t *paxos);
void              paxos_propose             (paxos_t *self,
                                             uint64_t value);
paxos_timeout_t * paxos_timeout             (paxos_t *paxos);
void              paxos_process_message     (paxos_t *paxos,
                                             const paxos_message_t *message);

#endif /* !_PAXOS_H_ */

