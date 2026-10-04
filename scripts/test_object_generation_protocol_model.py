#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Bounded design model, not RustFS's lock, disk, or RPC implementation.

Run with ``python3 scripts/test_object_generation_protocol_model.py``. The
exhaustive check covers one successor slot, four durable voters, two distinct
ballots/candidates, and every enabled prepare/accept ordering in that bound.
Durable transitions are atomic; filesystem crash atomicity is an implementation
obligation, not something this model can establish.
"""

from collections import deque
from dataclasses import dataclass, replace
import unittest


VOTERS = 4
QUORUM = 3
EMPTY = -1


@dataclass(frozen=True)
class Voter:
    promise: int = 0
    accepted_ballot: int = 0
    accepted_value: int = EMPTY

    def prepare(self, ballot):
        if ballot < self.promise:
            return self, None
        persisted = replace(self, promise=ballot)
        return persisted, (persisted.accepted_ballot, persisted.accepted_value)

    def accept(self, ballot, value):
        if ballot < self.promise:
            return self, False
        if self.accepted_ballot == ballot and self.accepted_value != value:
            return self, False
        return Voter(ballot, ballot, value), True


@dataclass(frozen=True)
class Proposer:
    prepare_attempts: int = 0
    replies: tuple = ()
    value: int = EMPTY
    accept_attempts: int = 0
    accepts: int = 0


@dataclass(frozen=True)
class State:
    voters: tuple = (Voter(),) * VOTERS
    proposers: tuple = (Proposer(), Proposer())
    chosen: int = 0


def successors(state, adopt_accepted=True):
    """A reply is sent only after persistence; arbitrary losses are tested below.

    The first successful promise quorum fixes the candidate. A failed request
    need not retry in this bounded run. Preparing or accepting at a higher
    ballot after a restart is a separate recovery run.
    """
    for index, proposer in enumerate(state.proposers):
        ballot = index + 1
        for disk in range(VOTERS):
            bit = 1 << disk
            voters = list(state.voters)
            if proposer.value == EMPTY:
                if proposer.prepare_attempts & bit:
                    continue
                voters[disk], reply = voters[disk].prepare(ballot)
                replies = proposer.replies + ((reply,) if reply is not None else ())
                value = EMPTY
                if len(replies) == QUORUM:
                    accepted = max(replies)
                    value = accepted[1] if adopt_accepted and accepted[0] else index
                updated = replace(proposer, prepare_attempts=proposer.prepare_attempts | bit, replies=replies, value=value)
                event = (index, "prepare", disk)
                chosen = state.chosen
            else:
                if proposer.accept_attempts & bit:
                    continue
                voters[disk], accepted = voters[disk].accept(ballot, proposer.value)
                accepts = proposer.accepts | bit if accepted else proposer.accepts
                updated = replace(proposer, accept_attempts=proposer.accept_attempts | bit, accepts=accepts)
                chosen = state.chosen
                if accepts.bit_count() >= QUORUM:
                    chosen |= 1 << proposer.value
                event = (index, "accept", disk)
            proposers = list(state.proposers)
            proposers[index] = updated
            yield State(tuple(voters), tuple(proposers), chosen), event


def explore(adopt_accepted=True):
    initial = State()
    queue = deque([initial])
    parents = {initial: None}
    transitions = 0
    terminal = 0
    chosen_values = set()
    while queue:
        state = queue.popleft()
        if state.chosen.bit_count() > 1:
            trace = []
            cursor = state
            while parents[cursor] is not None:
                previous, event = parents[cursor]
                trace.append(event)
                cursor = previous
            return len(parents), transitions, terminal, chosen_values, tuple(reversed(trace))
        if state.chosen:
            chosen_values.add(state.chosen)
        enabled = 0
        for successor, event in successors(state, adopt_accepted):
            enabled += 1
            transitions += 1
            if successor not in parents:
                parents[successor] = (state, event)
                queue.append(successor)
        terminal += enabled == 0
    return len(parents), transitions, terminal, chosen_values, None


def choose(voters, ballot, candidate, prepare_ids, accept_ids, adopt=True):
    """One recovery round; callers may deliberately lose every response."""
    for ids in (prepare_ids, accept_ids):
        if len(set(ids)) != len(ids) or any(disk < 0 or disk >= VOTERS for disk in ids):
            raise ValueError("a quorum contains distinct configured voters only")
    voters = list(voters)
    replies = []
    for disk in prepare_ids:
        voters[disk], reply = voters[disk].prepare(ballot)
        if reply is not None:
            replies.append(reply)
    if len(replies) < QUORUM:
        return tuple(voters), None, False
    accepted = max(replies)
    value = accepted[1] if adopt and accepted[0] else candidate
    count = 0
    for disk in accept_ids:
        voters[disk], success = voters[disk].accept(ballot, value)
        count += success
    return tuple(voters), value, count >= QUORUM


@dataclass(frozen=True)
class Disk:
    applied_revision: int = 0
    operation: str = "bootstrap"
    referenced: frozenset = frozenset()

    def publish_chosen(self, revision, operation, directories):
        if revision < self.applied_revision:
            return self, False
        next_state = Disk(revision, operation, frozenset(directories))
        if revision == self.applied_revision and next_state != self:
            return self, False
        return next_state, True

    def rollback_unchosen(self, operation, chosen):
        return not chosen and operation == self.operation

    def may_retire(self, directory, authorized, reader_protected, accepted_references):
        return authorized and directory not in self.referenced and directory not in accepted_references and not reader_protected


class ObjectGenerationProtocolModel(unittest.TestCase):
    def test_all_bounded_two_proposer_interleavings_choose_at_most_one_value(self):
        states, transitions, terminal, values, violation = explore()
        self.assertIsNone(violation, violation)
        self.assertEqual(values, {1, 2}, "both original candidates must be reachable")
        self.assertGreater(terminal, 0)
        print(f"model: states={states}, transitions={transitions}, terminal={terminal}, chosen_candidates={len(values)}")

    def test_omitting_highest_accepted_adoption_has_a_double_choice_counterexample(self):
        _, _, _, _, violation = explore(adopt_accepted=False)
        self.assertIsNotNone(violation, "the model must detect removing the safety rule")
        print(f"negative control: double-choice trace={violation}")

    def test_lost_ack_and_restart_preserve_the_chosen_operation(self):
        voters, value, chosen = choose((Voter(),) * VOTERS, 1, 0, (0, 1, 2), (0, 1, 2))
        self.assertTrue(chosen)
        self.assertEqual(value, 0)
        # A reboot drops proposer memory/replies, never the voter's persisted state.
        restarted = tuple(Voter(v.promise, v.accepted_ballot, v.accepted_value) for v in voters)
        _, recovered, chosen = choose(restarted, 2, 1, (1, 2, 3), (1, 2, 3))
        self.assertTrue(chosen)
        self.assertEqual(recovered, 0)

    def test_minority_accepted_work_is_adopted_and_finished(self):
        voters, _, chosen = choose((Voter(),) * VOTERS, 1, 0, (0, 1, 2), (0, 1))
        self.assertFalse(chosen)
        _, recovered, chosen = choose(voters, 2, 1, (1, 2, 3), (1, 2, 3))
        self.assertTrue(chosen)
        self.assertEqual(recovered, 0)

    def test_equal_ballot_cannot_accept_different_bytes_and_lower_ballot_is_rejected(self):
        voter, accepted = Voter().accept(2, 0)
        self.assertTrue(accepted)
        self.assertEqual(voter.accept(2, 1), (voter, False))
        self.assertEqual(voter.accept(1, 0), (voter, False))

    def test_crash_after_promise_before_reply_does_not_restore_an_older_ballot(self):
        persisted, _ = Voter().prepare(2)
        rebooted = Voter(persisted.promise, persisted.accepted_ballot, persisted.accepted_value)
        self.assertEqual(rebooted.accept(1, 0), (rebooted, False))

    def test_replacing_voters_with_empty_state_can_choose_a_conflicting_value(self):
        voters, original, chosen = choose((Voter(),) * VOTERS, 1, 0, (0, 1, 2), (0, 1, 2))
        self.assertTrue(chosen)
        # This deliberately violates enrollment: restored voters must catch up,
        # rather than reuse the old identity with an empty promise/accepted log.
        replaced = (Voter(), Voter(), voters[2], voters[3])
        _, replacement, chosen = choose(replaced, 2, 1, (0, 1, 3), (0, 1, 3))
        self.assertTrue(chosen)
        self.assertNotEqual(original, replacement)

    def test_an_unavailable_decision_quorum_cannot_choose_a_candidate(self):
        _, candidate, chosen = choose((Voter(),) * VOTERS, 1, 0, (0, 1), (0, 1, 2, 3))
        self.assertIsNone(candidate)
        self.assertFalse(chosen)

    def test_duplicate_or_unconfigured_voters_cannot_form_a_quorum(self):
        for invalid_ids in ((0, 0, 0), (-1, 0, 1), (0, 1, VOTERS)):
            for prepare_ids, accept_ids in ((invalid_ids, (0, 1, 2)), ((0, 1, 2), invalid_ids)):
                with self.subTest(prepare=prepare_ids, accept=accept_ids):
                    with self.assertRaisesRegex(ValueError, "distinct configured voters"):
                        choose((Voter(),) * VOTERS, 1, 0, prepare_ids, accept_ids)

    def test_quorum_choice_does_not_require_an_uncontacted_disk_to_learn_it(self):
        voters, _, chosen = choose((Voter(),) * VOTERS, 2, 1, (0, 1, 2), (0, 1, 2))
        self.assertTrue(chosen)
        isolated, accepted = voters[3].accept(1, 0)
        self.assertTrue(accepted, "an isolated voter has not learned the newer promise")
        recovered = list(voters)
        recovered[3] = isolated
        _, value, chosen = choose(recovered, 3, 0, (1, 2, 3), (1, 2, 3))
        self.assertTrue(chosen)
        self.assertEqual(value, 1, "isolated older work cannot replace the chosen value")

    def test_stale_publication_rollback_and_gc_cannot_change_the_winner(self):
        winner, published = Disk().publish_chosen(2, "B", {"B-data"})
        self.assertTrue(published)
        self.assertEqual(winner.publish_chosen(1, "A", {"A-data"}), (winner, False))
        self.assertEqual(winner.publish_chosen(2, "A", {"A-data"}), (winner, False))
        self.assertFalse(winner.rollback_unchosen("A", chosen=False))
        self.assertFalse(winner.rollback_unchosen("B", chosen=True))
        self.assertFalse(winner.may_retire("B-data", True, False, frozenset()))
        self.assertFalse(winner.may_retire("old-data", True, True, frozenset()))
        self.assertFalse(winner.may_retire("accepted-data", True, False, {"accepted-data"}))
        self.assertTrue(winner.may_retire("old-data", True, False, frozenset()))

    def test_tombstone_and_null_marker_payloads_do_not_reset_authority_revision(self):
        current = Disk()
        for revision, operation, directories in ((1, "null", {"null-data"}), (2, "marker", set()), (3, "last-version-delete", set())):
            current, published = current.publish_chosen(revision, operation, directories)
            self.assertTrue(published)
            self.assertEqual(current.applied_revision, revision)
        self.assertEqual(current.publish_chosen(0, "bootstrap", set()), (current, False))


if __name__ == "__main__":
    unittest.main(verbosity=2)
