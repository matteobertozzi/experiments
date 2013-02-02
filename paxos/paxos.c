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

/*
 *   Client   Proposer      Acceptor     Learner
 *      |         |          |  |  |       |  |
 *      X-------->|          |  |  |       |  |  Request
 *      |         X--------->|->|->|       |  |  Prepare(1)
 *      |         |<---------X--X--X       |  |  Promise(1,{Va,Vb,Vc})
 *      |         X--------->|->|->|       |  |  Accept!(1,Vn)
 *      |         |<---------X--X--X------>|->|  Accepted(1,Vn)
 *      |<---------------------------------X--X  Response
 *      |         |          |  |  |       |  |
 */

#include <sys/time.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "paxos.h"

#define ASSERT(cond)                                                        \
  if (!(cond)) fprintf(stderr, "ASSERT %s\n", #cond)

#define PAXOS_ROUND_TIMEOUT     (5000)
#define PAXOS_CHOSEN_TIMEOUT    (PAXOS_ROUND_TIMEOUT + 1000)
#define PAXOS_RESTART_TIMEOUT   (1000)

#define PAXOS_IS_DEBUG_ENABLED  0
#if PAXOS_IS_DEBUG_ENABLED
  #define __log(frmt, ...)      fprintf(stderr, "%lu: %d %s: " frmt "\n",   \
                                        paxos_time_now(), __LINE__,         \
                                        __FUNCTION__, ##__VA_ARGS__)
  #define LOG_TRACE(frmt, ...)  __log("TRACE: " frmt, ##__VA_ARGS__)
  #define LOG_DEBUG(frmt, ...)  __log("DEBUG: " frmt, ##__VA_ARGS__)
  #define LOG_FUNC_TRACE        __log("");
#else
  #define LOG_TRACE(format, ...)
  #define LOG_DEBUG(format, ...)
  #define LOG_FUNC_TRACE
#endif

static void paxos_proposer_state_reset (paxos_proposer_state_t *self) {
  self->preparing = 0;
  self->proposing = 0;
  self->learn_sent = 0;
  self->proposal_id = 0;
  self->highest_received_proposal_id = 0;
  self->highest_promised_proposal_id = 0;
  self->proposed_value = 0;
}

static void paxos_acceptor_state_reset (paxos_acceptor_state_t *self) {
  self->promised_proposal_id = 0;
  self->accepted = 0;
  self->accepted_proposal_id = 0;
  self->accepted_value = 0;
}

/* ============================================================================
 *  Paxos Timer
 */
static uint64_t paxos_time_now (void) {
  struct timeval now;
  gettimeofday(&now, NULL);
  return(now.tv_sec * 1000 + (now.tv_usec / 1000));
}

void paxos_timeout_init (paxos_timeout_t *self,
                         unsigned int timeout,
                         paxos_callback_t callback,
                         void *arg)
{
  self->arg = arg;
  self->callback = callback;
  self->active = 0;
  self->timeout = timeout;
  self->expire_time = 0;
}

void paxos_timeout_start (paxos_timeout_t *self) {
  self->active = 1;
  self->expire_time = paxos_time_now() + self->timeout;
}

void paxos_timeout_stop (paxos_timeout_t *self) {
  self->active = 0;
}

unsigned int paxos_timeout_remaining (paxos_timeout_t *self) {
  int timeout = -1;
  if (self != NULL && self->active) {
    timeout = paxos_time_now() - self->expire_time;
  }
  return(timeout < 0 ? 1000 : timeout);
}

void paxos_timeout_trigger(paxos_timeout_t *self) {
  if (self != NULL && self->active) {
    self->active = 0;
    self->callback(self->arg);
  }
}

/* ============================================================================
 *  Paxos Message
 */
static void __paxos_message_paxos_id (paxos_message_t *message,
                                      uint8_t type,
                                      uint64_t paxos_id,
                                      uint64_t node_id)
{
  memset(message, 0, sizeof(paxos_message_t));
  message->type = type;
  message->paxos_id = paxos_id;
  message->node_id = node_id;
}

static void __paxos_message_proposal_id (paxos_message_t *message,
                                         uint8_t type,
                                         uint64_t paxos_id,
                                         uint64_t node_id,
                                         uint64_t proposal_id)
{
  memset(message, 0, sizeof(paxos_message_t));
  message->type = type;
  message->paxos_id = paxos_id;
  message->node_id = node_id;
  message->proposal_id = proposal_id;
}

#define paxos_message_request_chosen(msg, paxos_id, node_id)                \
  __paxos_message_paxos_id(msg, PAXOS_REQUEST_CHOSEN, paxos_id, node_id)

#define paxos_message_bootstrap(msg, node_id)                               \
  __paxos_message_paxos_id(msg, PAXOS_BOOTSTRAP, 0, node_id)

#define paxos_message_catchup_start(msg, paxos_id, node_id)                 \
  __paxos_message_paxos_id(msg, PAXOS_CATCHUP_START, paxos_id, node_id)

#define paxos_message_catchup_request(msg, paxos_id, node_id)               \
  __paxos_message_paxos_id(msg, PAXOS_CATCHUP_REQUEST, paxos_id, node_id)


#define paxos_message_prepare_request(msg, paxos_id, node_id, proposal_id)  \
  __paxos_message_proposal_id(msg, PAXOS_PREPARE_REQUEST,                   \
                              paxos_id, node_id, proposal_id)

#define paxos_message_prepare_currently_open(msg, paxos_id, node_id, proposal) \
  __paxos_message_proposal_id(msg, PAXOS_PREPARE_CURRENTLY_OPEN,               \
                              paxos_id, node_id, proposal)

#define paxos_message_propose_accepted(msg, paxos_id, node_id, proposal_id)   \
  __paxos_message_proposal_id(msg, PAXOS_PROPOSE_ACCEPTED,                    \
                              paxos_id, node_id, proposal_id)

#define paxos_message_propose_rejected(msg, paxos_id, node_id, proposal_id)   \
  __paxos_message_proposal_id(msg, PAXOS_PROPOSE_REJECTED,                    \
                              paxos_id, node_id, proposal_id)

#define paxos_message_learn_proposal(msg, paxos_id, node_id, proposal_id)     \
  __paxos_message_proposal_id(msg, PAXOS_LEARN_PROPOSAL,                      \
                              paxos_id, node_id, proposal_id)

void paxos_message_catchup_response (paxos_message_t *message,
                                     uint64_t paxos_id,
                                     uint64_t node_id,
                                     uint64_t value)
{
  memset(message, 0, sizeof(paxos_message_t));
  message->type = PAXOS_CATCHUP_RESPONSE;
  message->paxos_id = paxos_id;
  message->node_id = node_id;
  message->value = value;
}

void paxos_message_learn_value (paxos_message_t *message,
                                uint64_t paxos_id,
                                uint64_t node_id,
                                uint64_t value)
{
  memset(message, 0, sizeof(paxos_message_t));
  message->type = PAXOS_CATCHUP_START;
  message->paxos_id = paxos_id;
  message->node_id = node_id;
  message->value = value;
}

void paxos_message_propose_request (paxos_message_t *message,
                                    uint64_t paxos_id,
                                    uint64_t node_id,
                                    uint64_t proposal_id,
                                    uint64_t value)
{
  memset(message, 0, sizeof(paxos_message_t));
  message->type = PAXOS_PROPOSE_REQUEST;
  message->paxos_id = paxos_id;
  message->node_id = node_id;
  message->proposal_id = proposal_id;
  message->value = value;
}

void paxos_message_prepare_previously_accepted (paxos_message_t *message,
                                                uint64_t paxos_id,
                                                uint64_t node_id,
                                                uint64_t proposal_id,
                                                uint64_t accepted_proposal_id,
                                                uint64_t value)
{
  memset(message, 0, sizeof(paxos_message_t));
  message->type = PAXOS_PREPARE_PREVIOUSLY_ACCEPTED;
  message->paxos_id = paxos_id;
  message->node_id = node_id;
  message->proposal_id = proposal_id;
  message->accepted_proposal_id = accepted_proposal_id;
  message->value = value;
}

void paxos_message_prepare_rejected (paxos_message_t *message,
                                     uint64_t paxos_id,
                                     uint64_t node_id,
                                     uint64_t proposal_id,
                                     uint64_t promised_proposal_id)
{
  memset(message, 0, sizeof(paxos_message_t));
  message->type = PAXOS_PREPARE_REJECTED;
  message->paxos_id = paxos_id;
  message->node_id = node_id;
  message->proposal_id = proposal_id;
  message->promised_proposal_id = promised_proposal_id;
}

const char *paxos_message_to_string (const paxos_message_t *message) {
  switch (message->type) {
    case PAXOS_PREPARE_REQUEST: return("prepare-request");
    case PAXOS_PREPARE_REJECTED: return("prepare-rejected");
    case PAXOS_PREPARE_PREVIOUSLY_ACCEPTED: return("prepared-previously-accepted");
    case PAXOS_PREPARE_CURRENTLY_OPEN: return("prepare-currently-open");
    case PAXOS_PROPOSE_REQUEST: return("propose-request");
    case PAXOS_PROPOSE_REJECTED: return("propose-rejected");
    case PAXOS_PROPOSE_ACCEPTED: return("propose-accepted");
    case PAXOS_LEARN_PROPOSAL: return("learn-proposal");
    case PAXOS_LEARN_VALUE: return("learn-value");
    case PAXOS_REQUEST_CHOSEN: return("request-chosen");
    case PAXOS_BOOTSTRAP: return("bootstrap");
    case PAXOS_CATCHUP_START: return("start-catchup");
    case PAXOS_CATCHUP_REQUEST: return("catchup-request");
    case PAXOS_CATCHUP_RESPONSE: return("catchup-response");
    case PAXOS_USER_PROPOSE_VALUE: return("user-propoe-value");
    case PAXOS_USER_LEARN_VALUE: return("user-learn-value");
  }
  return("");
}

/* ============================================================================
 *  Paxos Quorum
 */
#define __math_ceil(a, b)    ((a) + (b) - 1) / (b)
#define __math_max(a, b)     ((a) > (b) ? (a) : (b))

#define paxos_quorum_vote_reset(self)                                     \
  (self)->num_rejected = (self)->num_accepted = 0


#define paxos_quorum_vote_accepted(self, node_id)                         \
  (self)->num_accepted++

#define paxos_quorum_vote_rejected(self, node_id)                         \
  (self)->num_rejected++

#define paxos_quorum_vote_is_rejected(self)                               \
  ((self)->num_rejected >= __math_ceil((self)->num_nodes, 2))

#define paxos_quorum_vote_is_accepted(self)                               \
  ((self)->num_accepted >= __math_ceil((self)->num_nodes + 1, 2))

#define paxos_quorum_vote_is_complete(self)                               \
  (((self)->num_accepted + (self)->num_rejected) >= (self)->num_nodes)

/* ============================================================================
 *  Paxos Context
 */
#define paxos_context_send(self, node_id, message)                        \
  (self)->send((self)->arg, node_id, message)

#define paxos_context_broadcast(self, message)                            \
  (self)->broadcast((self)->arg, message)

#define paxos_context_learned_value(self)                                 \
  if ((self)->learned_value != NULL) (self)->learned_value((self)->arg)

/* ============================================================================
 *  Paxos Helpers
 */
static void paxos_commit (paxos_t *self, paxos_callback_t callback, void *arg) {
  LOG_DEBUG("TODO: DO COMMIT");
  /* TODO: write to WAL */
  callback(arg);
}

static int paxos_get_accepted_value (paxos_t *self,
                                     uint64_t paxos_id,
                                     uint64_t *value)
{
  if (self->learner.paxos_id == paxos_id) {
    *value = self->learner.learned_value;
    return(1);
  }
  return(0);
}

static void paxos_start_new_round (paxos_t *self) {
  self->learner.paxos_id++;
  paxos_proposer_state_reset(&(self->proposer.state));
  paxos_acceptor_state_reset(&(self->acceptor.state));
}

#define __paxos_learned_timeout(self)                                       \
  (paxos_time_now() - (self)->learner.last_request_chosen_time)

#define paxos_is_blocked(self)                                              \
  (__paxos_learned_timeout(self) > PAXOS_CHOSEN_TIMEOUT)

/* ============================================================================
 *  Paxos Learner
 */
static void paxos_learner_learn_value (paxos_t *self, uint64_t value) {
  /* Update the learned value */
  self->learner.learned_value = value;
  self->learner.has_learned_value = 1;

#if PAXOS_IS_DEBUG_ENABLED
  fprintf(stderr, "========================================================\n");
  fprintf(stderr, "%lu: LEARNED VALUE %lu PAXOS %lu\n", paxos_time_now(),
          self->learner.learned_value, self->learner.paxos_id);
  fprintf(stderr, "========================================================\n");
#endif

  /* Notify the user about the new value */
  paxos_context_learned_value(self->context);
}

static void __on_request_chosen (paxos_t *self, const paxos_message_t *message)
{
  paxos_message_t omsg;
  uint64_t value;

  if (message->paxos_id >= self->learner.paxos_id)
    return;

  if (paxos_get_accepted_value(self, message->paxos_id, &value)) {
      LOG_TRACE("Sending PaxosID %lu to node %lu",
                message->paxos_id, message->node_id);
      paxos_message_learn_value(&omsg, message->paxos_id, self->node_id, value);
  } else {
      LOG_TRACE("PaxosID not found, start catchup!");
      paxos_message_catchup_start(&omsg, self->learner.paxos_id, self->node_id);
  }

  paxos_context_send(self->context, message->node_id, &omsg);
}

static void paxos_learner_init (paxos_t *paxos, paxos_learner_t *learner) {
  learner->paxos_id = 0;
  learner->has_learned_value = 0;
  learner->last_request_chosen_time = 0;
}

/* ============================================================================
 *  Paxos Acceptor
 */
struct paxos_commit_info {
  paxos_t *paxos;
  const paxos_message_t *message;
};

static void __on_state_written (void *arg) {
  struct paxos_commit_info *commit_info = (struct paxos_commit_info *)arg;
  paxos_t *paxos = commit_info->paxos;

  LOG_FUNC_TRACE
  paxos->acceptor.is_committing = 0;

  if (paxos->acceptor.written_paxos_id == paxos->learner.paxos_id) {
    paxos_context_send(paxos->context,
                       paxos->acceptor.sender_id,
                       commit_info->message);
  }
}

static void __commit (paxos_t *paxos,
                      paxos_acceptor_t *acceptor,
                      const paxos_message_t *message)
{
  struct paxos_commit_info commit_info;

  LOG_FUNC_TRACE
  ASSERT(!acceptor->is_committing);

  acceptor->written_paxos_id = paxos->learner.paxos_id;
  acceptor->is_committing = 1;

  commit_info.paxos = paxos;
  commit_info.message = message;
  paxos_commit(paxos, __on_state_written, &commit_info);
}

static void __accept_prepare_request (paxos_t *paxos,
                                      paxos_acceptor_t *acceptor,
                                      const paxos_message_t *message)
{
  paxos_message_t omsg;

  LOG_FUNC_TRACE

  acceptor->state.promised_proposal_id = message->proposal_id;

  acceptor->sender_id = message->node_id;
  if (!acceptor->state.accepted) {
    paxos_message_prepare_currently_open(&omsg, message->paxos_id,
                                         paxos->node_id,
                                         message->proposal_id);
  } else {
    paxos_message_prepare_previously_accepted(&omsg,
                                          message->paxos_id,
                                          paxos->node_id,
                                          message->proposal_id,
                                          acceptor->state.accepted_proposal_id,
                                          acceptor->state.accepted_value);
  }

  __commit(paxos, acceptor, &omsg);
}

static void __accept_propose_request (paxos_t *paxos,
                                      paxos_acceptor_t *acceptor,
                                      const paxos_message_t *message)
{
  paxos_message_t omsg;

  LOG_FUNC_TRACE

  acceptor->state.accepted = 1;
  acceptor->state.accepted_proposal_id = message->proposal_id;
  acceptor->state.accepted_value = message->value;
  LOG_TRACE("proposal_id=%lu value=%lu",
            acceptor->state.accepted_proposal_id,
            acceptor->state.accepted_value);

  acceptor->sender_id = message->node_id;
  paxos_message_propose_accepted(&omsg, message->paxos_id,
                                 paxos->node_id,
                                 message->proposal_id);

  __commit(paxos, acceptor, &omsg);
}

static int __can_accept_request (paxos_t *paxos,
                                 paxos_acceptor_t *acceptor,
                                 const paxos_message_t *message)
{
  LOG_FUNC_TRACE

  if (message->paxos_id != paxos->learner.paxos_id)
    return(0);

  if (message->proposal_id < acceptor->state.promised_proposal_id)
    return(0);

  if (acceptor->is_committing)
    return(0);

  return(1);
}

static void __on_prepare_request (paxos_t *paxos,
                                  paxos_acceptor_t *acceptor,
                                  const paxos_message_t *message)
{
  LOG_FUNC_TRACE

  if (__can_accept_request(paxos, acceptor, message)) {
    __accept_prepare_request(paxos, acceptor, message);
  } else {
    paxos_message_t omsg;
    paxos_message_prepare_rejected(&omsg, message->paxos_id,
                                   paxos->node_id,
                                   message->proposal_id,
                                   acceptor->state.promised_proposal_id);
    paxos_context_send(paxos->context, message->node_id, &omsg);
  }
}

static void __on_propose_request (paxos_t *paxos,
                                  paxos_acceptor_t *acceptor,
                                  const paxos_message_t *message)
{
  LOG_FUNC_TRACE

  if (__can_accept_request(paxos, acceptor, message)) {
    __accept_propose_request(paxos, acceptor, message);
  } else {
    paxos_message_t omsg;
    paxos_message_propose_rejected(&omsg, message->paxos_id,
                                   paxos->node_id,
                                   message->proposal_id);
    paxos_context_send(paxos->context, message->node_id, &omsg);
  }
}

static void __request_chosen (paxos_t *paxos,
                              paxos_learner_t *learner,
                              uint64_t node_id)
{
  paxos_message_t omsg;
  learner->last_request_chosen_time = paxos_time_now();
  paxos_message_request_chosen(&omsg, learner->paxos_id, paxos->node_id);
  paxos_context_send(paxos->context, node_id, &omsg);
}

static void __on_learn_chosen (paxos_t *paxos,
                               paxos_acceptor_t *acceptor,
                               const paxos_message_t *message)
{
  LOG_FUNC_TRACE

  if (acceptor->is_committing)
    return;

  if (message->paxos_id > paxos->learner.paxos_id) {
    __request_chosen(paxos, &(paxos->learner), message->node_id);
    return;
  }

  if (message->paxos_id < paxos->learner.paxos_id)
    return;

  if (message->type == PAXOS_LEARN_VALUE) {
    acceptor->state.accepted = 1;
    acceptor->state.accepted_value = message->value;
  } else if (!(message->type == PAXOS_LEARN_PROPOSAL &&
               acceptor->state.accepted &&
               acceptor->state.accepted_proposal_id == message->proposal_id))
  {
    __request_chosen(paxos, &(paxos->learner), message->node_id);
    return;
  }

  /* Set learned value and start new paxos round */
  paxos_learner_learn_value(paxos, acceptor->state.accepted_value);
  paxos_start_new_round(paxos);
}

static void paxos_acceptor_init (paxos_t *paxos, paxos_acceptor_t *acceptor) {
  acceptor->is_committing = 0;
  acceptor->sender_id = 0;
  acceptor->written_paxos_id = 0;
  paxos_acceptor_state_reset(&(acceptor->state));
}

/* ============================================================================
 *  Paxos Proposer
 */
static void __stop_preparing (paxos_t *paxos, paxos_proposer_t *proposer) {
  LOG_FUNC_TRACE
  proposer->state.preparing = 0;
  paxos_timeout_stop(&(proposer->prepare_timeout));
}

static void __start_proposing (paxos_t *paxos, paxos_proposer_t *proposer) {
  paxos_message_t omsg;

  LOG_FUNC_TRACE
  __stop_preparing(paxos, proposer);

  paxos_quorum_vote_reset(&(paxos->quorum));
  proposer->state.proposing = 1;

  paxos_message_propose_request(&omsg, paxos->learner.paxos_id,
                                paxos->node_id,
                                proposer->state.proposal_id,
                                proposer->state.proposed_value);
  paxos_context_broadcast(paxos->context, &omsg);

  paxos_timeout_stop(&(proposer->restart_timeout));
  paxos_timeout_start(&(proposer->propose_timeout));
}

static void __stop_proposing (paxos_t *paxos, paxos_proposer_t *proposer) {
  LOG_FUNC_TRACE
  proposer->state.proposing = 0;
  paxos_timeout_stop(&(proposer->propose_timeout));
}

static uint64_t __next_proposal_id (paxos_t *self, paxos_proposer_t *proposer) {
  return(1 + __math_max(proposer->state.proposal_id,
                        proposer->state.highest_promised_proposal_id));
}

static void __start_preparing (paxos_t *paxos, paxos_proposer_t *proposer) {
  paxos_message_t omsg;

  LOG_FUNC_TRACE

  __stop_proposing(paxos, proposer);

  paxos_quorum_vote_reset(&(paxos->quorum));
  proposer->state.preparing = 1;
  proposer->state.proposal_id = __next_proposal_id(paxos, proposer);
  proposer->state.highest_received_proposal_id = 0;

  paxos_message_prepare_request(&omsg, paxos->learner.paxos_id,
                                paxos->node_id, proposer->state.proposal_id);
  paxos_context_broadcast(paxos->context, &omsg);

  paxos_timeout_stop(&(proposer->restart_timeout));
  paxos_timeout_start(&(proposer->prepare_timeout));
}

static void __on_prepare_response (paxos_t *paxos,
                                   paxos_proposer_t *proposer,
                                   const paxos_message_t *message)
{
  LOG_FUNC_TRACE

  if (!proposer->state.preparing ||
      message->proposal_id != proposer->state.proposal_id)
  {
    return;
  }

  if (message->type == PAXOS_PREPARE_REJECTED) {
    paxos_quorum_vote_rejected(&(paxos->quorum), message->node_id);
  } else {
    paxos_quorum_vote_accepted(&(paxos->quorum), message->node_id);
  }

  if (message->type == PAXOS_PREPARE_PREVIOUSLY_ACCEPTED &&
      message->accepted_proposal_id >= proposer->state.highest_received_proposal_id)
  {
    proposer->state.highest_received_proposal_id = message->accepted_proposal_id;
    proposer->state.proposed_value = message->value;
  } else if (message->type == PAXOS_PREPARE_REJECTED) {
    if (message->promised_proposal_id > proposer->state.highest_promised_proposal_id)
        proposer->state.highest_promised_proposal_id = message->promised_proposal_id;
  }

  if (paxos_quorum_vote_is_accepted(&(paxos->quorum))) {
    __start_proposing(paxos, proposer);
  } else if (paxos_quorum_vote_is_rejected(&(paxos->quorum))) {
    __stop_preparing(paxos, proposer);
    paxos_timeout_start(&(proposer->restart_timeout));
  }
}

static void __on_propose_response (paxos_t *paxos,
                                   paxos_proposer_t *proposer,
                                   const paxos_message_t *message)
{
  paxos_message_t omsg;

  LOG_FUNC_TRACE

  if (!proposer->state.proposing || message->proposal_id != proposer->state.proposal_id)
    return;

  if (message->type == PAXOS_PROPOSE_REJECTED) {
    paxos_quorum_vote_rejected(&(paxos->quorum), message->node_id);
  } else {
    paxos_quorum_vote_accepted(&(paxos->quorum), message->node_id);
  }

  if (paxos_quorum_vote_is_accepted(&(paxos->quorum))) {
    __stop_proposing(paxos, proposer);
    paxos_message_learn_proposal(&omsg, paxos->learner.paxos_id,
                                 paxos->node_id,
                                 proposer->state.proposal_id);
    paxos_context_broadcast(paxos->context, &omsg);
    proposer->state.learn_sent = 1;
  } else if (paxos_quorum_vote_is_rejected(&(paxos->quorum))) {
    __stop_proposing(paxos, proposer);
    paxos_timeout_start(&(proposer->restart_timeout));
  }
}

static void __on_prepare_timeout (void *arg) {
  paxos_t *paxos = (paxos_t *)arg;
  int is_blocked;

  LOG_FUNC_TRACE
  ASSERT(paxos->proposer.state.preparing);

  is_blocked = paxos_is_blocked(paxos);
  if (is_blocked || paxos_quorum_vote_is_rejected(&(paxos->quorum))) {
    __start_preparing(paxos, &(paxos->proposer));
  } else {
    paxos_timeout_start(&(paxos->proposer.prepare_timeout));
  }
}

static void __on_propose_timeout (void *arg) {
  paxos_t *paxos = (paxos_t *)arg;
  int is_blocked;

  LOG_FUNC_TRACE
  ASSERT(paxos->proposer.state.proposing);

  is_blocked = paxos_is_blocked(paxos);
  if (is_blocked || paxos_quorum_vote_is_rejected(&(paxos->quorum))) {
    __start_preparing(paxos, &(paxos->proposer));
  } else {
    paxos_timeout_start(&(paxos->proposer.propose_timeout));
  }
}

static void __on_restart_timeout (void *arg) {
  paxos_t *paxos = (paxos_t *)arg;

  LOG_FUNC_TRACE
  LOG_DEBUG("OnRestart_timeout");

  ASSERT(!paxos->proposer.state.preparing);
  ASSERT(!paxos->proposer.state.proposing);

  if (paxos_is_blocked(paxos)) {
    __start_preparing(paxos, &(paxos->proposer));
  } else {
    paxos_timeout_start(&(paxos->proposer.restart_timeout));
  }
}

static void paxos_proposer_init (paxos_t *paxos, paxos_proposer_t *proposer) {
  paxos_proposer_state_reset(&(proposer->state));
  paxos_timeout_init(&(proposer->prepare_timeout),
                     PAXOS_ROUND_TIMEOUT, __on_prepare_timeout, paxos);
  paxos_timeout_init(&(proposer->propose_timeout),
                     PAXOS_ROUND_TIMEOUT, __on_propose_timeout, paxos);
  paxos_timeout_init(&(proposer->restart_timeout),
                     PAXOS_RESTART_TIMEOUT, __on_restart_timeout, paxos);
}

static void paxos_proposer_stop (paxos_proposer_t *proposer) {
  paxos_proposer_state_reset(&(proposer->state));
  paxos_timeout_stop(&(proposer->prepare_timeout));
  paxos_timeout_stop(&(proposer->propose_timeout));
  paxos_timeout_stop(&(proposer->restart_timeout));
}

#define paxos_proposer_is_active(proposer)                                  \
  ((self)->state.preparing ||                                               \
   (self)->state.proposing ||                                               \
   (self)->restart_timeout.active)

#define paxos_proposer_is_learn_sent(proposer)                              \
  ((proposer)->state.learn_sent)

static void paxos_proposer_propose (paxos_t *paxos,
                                    paxos_proposer_t *proposer,
                                    uint64_t value)
{
  proposer->state.proposed_value = value;
#if 1
    __start_preparing(paxos, proposer);
#else
    /* Multi Paxos, skip the preparing and go directly with the proposal */
    __start_proposing(proposer);
#endif
}

/* ============================================================================
 *  Paxos Bootstra/Catchup
 */
static void __on_bootstrap (paxos_t *self, const paxos_message_t *message) {
  paxos_message_t omsg;
  uint64_t value;

  if (!self->learner.has_learned_value)
    return;

  fprintf(stderr, "bootstrap\n");
  if (paxos_get_accepted_value(self, self->learner.paxos_id, &value)) {
    paxos_message_catchup_response(&omsg, self->learner.paxos_id, self->node_id, value);
    paxos_context_send(self->context, message->node_id, &omsg);
  }
}

static void __on_catchup_start (paxos_t *self, const paxos_message_t *message) {
  paxos_message_t omsg;

  if (self->node_id == message->node_id)
    return;

  LOG_DEBUG("paxos_id: %lu node: %lu\n",
            message->paxos_id, message->node_id);
  paxos_message_catchup_request(&omsg, message->paxos_id, self->node_id);
  paxos_context_send(self->context, message->node_id, &omsg);
}

static void __on_catchup_request (paxos_t *self, const paxos_message_t *message) {
  paxos_message_t omsg;
  uint64_t value;

  LOG_DEBUG("paxos_id: %lu node: %lu\n", message->paxos_id, message->node_id);
  if (paxos_get_accepted_value(self, message->paxos_id, &value)) {
    paxos_message_catchup_response(&omsg, message->paxos_id, self->node_id, value);
    paxos_context_send(self->context, message->node_id, &omsg);
  }
}

static void __on_catchup_response (paxos_t *self,
                                   paxos_learner_t *learner,
                                   const paxos_message_t *message)
{
  if (learner->has_learned_value && learner->paxos_id >= message->paxos_id)
    return;

  LOG_DEBUG("paxos_id: %lu node: %lu\n", message->paxos_id, message->node_id);
  learner->paxos_id = message->paxos_id;
  paxos_learner_learn_value(self, message->value);

  paxos_proposer_state_reset(&(self->proposer.state));
  paxos_acceptor_state_reset(&(self->acceptor.state));
}

/* ============================================================================
 *  Paxos
 */
void paxos_open (paxos_t *self,
                 paxos_context_t *context,
                 uint64_t node_id,
                 uint64_t num_nodes)
{
  self->context = context;
  self->quorum.num_nodes = num_nodes;
  self->node_id = node_id;
  paxos_proposer_init(self, &(self->proposer));
  paxos_acceptor_init(self, &(self->acceptor));
  paxos_learner_init(self, &(self->learner));
}

void paxos_close (paxos_t *self) {
  paxos_proposer_stop(&(self->proposer));
}

void paxos_bootstrap (paxos_t *self) {
  paxos_message_t omsg;
  paxos_message_bootstrap(&omsg, self->node_id);
  paxos_context_broadcast(self->context, &omsg);
}

void paxos_propose (paxos_t *self, uint64_t value) {
  paxos_proposer_propose(self, &(self->proposer), value);
}

paxos_timeout_t *paxos_timeout (paxos_t *self) {
  paxos_timeout_t *min_timeout = NULL;

  #define __select_min_timeout(x)                         \
    if ((x)->active && (min_timeout == NULL ||            \
        (x)->expire_time < min_timeout->expire_time))     \
      min_timeout = x;

  __select_min_timeout(&(self->proposer.prepare_timeout));
  __select_min_timeout(&(self->proposer.propose_timeout));
  __select_min_timeout(&(self->proposer.restart_timeout));
  return(min_timeout);
}

void paxos_process_message (paxos_t *paxos, const paxos_message_t *message) {
  LOG_FUNC_TRACE

  switch (message->type) {
    /* Prepare Request */
    case PAXOS_PREPARE_REQUEST:
      __on_prepare_request(paxos, &(paxos->acceptor), message);
      break;
    /* Prepare Response */
    case PAXOS_PREPARE_REJECTED:
    case PAXOS_PREPARE_PREVIOUSLY_ACCEPTED:
    case PAXOS_PREPARE_CURRENTLY_OPEN:
      __on_prepare_response(paxos, &(paxos->proposer), message);
      break;
    /* Propose Request */
    case PAXOS_PROPOSE_REQUEST:
      __on_propose_request(paxos, &(paxos->acceptor), message);
      break;
    /* Propose Response */
    case PAXOS_PROPOSE_REJECTED:
    case PAXOS_PROPOSE_ACCEPTED:
      __on_propose_response(paxos, &(paxos->proposer), message);
      break;
    /* Is learn */
    case PAXOS_LEARN_PROPOSAL:
    case PAXOS_LEARN_VALUE:
      __on_learn_chosen(paxos, &(paxos->acceptor), message);
      break;
    /* Request Chosen */
    case PAXOS_REQUEST_CHOSEN:
      __on_request_chosen(paxos, message);
      break;
    /* Catch-up */
    case PAXOS_BOOTSTRAP:
      __on_bootstrap(paxos, message);
      break;
    case PAXOS_CATCHUP_START:
      __on_catchup_start(paxos, message);
      break;
    case PAXOS_CATCHUP_REQUEST:
      __on_catchup_request(paxos, message);
      break;
    case PAXOS_CATCHUP_RESPONSE:
      __on_catchup_response(paxos, &(paxos->learner), message);
      break;
    /* Invalid message */
    default:
      fprintf(stderr, "paxos: invalid message %u\n", message->type);
      break;
  }
}
