#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Bounded design model, not RustFS's lock, disk, or RPC implementation.

Run with ``python3 scripts/test_object_generation_protocol_model.py``. The
exhaustive check covers one successor slot, four durable voters, two distinct
ballots/candidates, and every enabled prepare/accept ordering in that bound.
Directed round schedules also cover successor recovery, tombstone continuity,
and proposer counter allocation; they do not exhaust multi-slot interleavings.
Durable transitions are atomic; filesystem crash atomicity is an implementation
obligation, not something this model can establish.
"""

from collections import deque
from dataclasses import dataclass, replace
from enum import Enum
from itertools import combinations, product
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
    if value == EMPTY:
        # A recovery barrier with no accepted value does not choose an empty head.
        return tuple(voters), None, False
    count = 0
    for disk in accept_ids:
        voters[disk], success = voters[disk].accept(ballot, value)
        count += success
    return tuple(voters), value, count >= QUORUM


class Presence(Enum):
    NEVER_PRESENT = "never-present"
    LIVE = "live"
    TOMBSTONE = "tombstone"


@dataclass(frozen=True)
class Head:
    revision: int = 0
    presence: Presence = Presence.NEVER_PRESENT
    operation: str = "bootstrap"


@dataclass(frozen=True)
class Decision:
    predecessor: Head
    successor: Head


@dataclass(frozen=True)
class AuthorityHistory:
    slots: tuple = ()
    decisions: tuple = ()
    learned: Head = Head()

    def append(
        self, expected, operation, presence, ballot, prepare_ids, accept_ids, *, learn=True,
        recover_first=True, collapse_tombstone=False,
    ):
        slots = list(self.slots)
        head = self.learned
        if recover_first:
            for index in range(head.revision, len(slots)):
                # Re-propose this slot's retained preparation if the promise
                # quorum observed no accepted value; otherwise adopt its winner.
                slots[index], value, resolved = choose(slots[index], ballot, index, prepare_ids, accept_ids)
                if not resolved:
                    return replace(self, slots=tuple(slots), learned=head), None, False
                decision = self.decisions[value]
                if decision.predecessor != head or decision.successor.revision != index + 1:
                    raise ValueError("recovered decisions must extend the chosen predecessor")
                head = decision.successor
        if collapse_tombstone and head.presence is Presence.TOMBSTONE:
            head = Head()
        recovered = replace(self, slots=tuple(slots), learned=head)
        for original in self.decisions[:head.revision]:
            if original.successor.operation == operation:
                if original.predecessor == expected and original.successor.presence is presence:
                    return recovered, original.successor, True
                return recovered, None, False
        if expected != head:
            return recovered, None, False
        if presence is Presence.NEVER_PRESENT:
            raise ValueError("a successor cannot restore never-present authority")
        successor = Head(head.revision + 1, presence, operation)
        decisions = self.decisions + (Decision(head, successor),)
        slots.append((Voter(),) * VOTERS)
        slots[-1], value, chosen = choose(slots[-1], ballot, len(decisions) - 1, prepare_ids, accept_ids)
        learned = successor if chosen and learn else head
        updated = AuthorityHistory(tuple(slots), decisions, learned)
        return updated, decisions[value].successor if chosen else None, chosen


@dataclass(frozen=True)
class ProposerSession:
    # Only this counter survives a restart; reservation is process-local.
    durable_counter: int = 0
    reservation: int = 0

    def reserve(self, observed_counter=0):
        if self.reservation:
            raise ValueError("a ballot allocation is already pending")
        return replace(self, reservation=max(self.durable_counter, observed_counter) + 1)

    def persist(self):
        if not self.reservation:
            raise ValueError("no ballot allocation to persist")
        return replace(self, durable_counter=self.reservation)

    def issue(self, *, require_persistence=True):
        if not self.reservation:
            raise ValueError("no ballot allocation to issue")
        if require_persistence and self.durable_counter != self.reservation:
            raise ValueError("persist the proposer counter before using a ballot")
        return replace(self, reservation=0), self.reservation

    def restart(self):
        return ProposerSession(self.durable_counter)


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

    def test_unlearned_predecessor_is_recovered_before_the_second_slot(self):
        quorums = tuple(combinations(range(VOTERS), QUORUM))
        first_accepts = quorums + tuple(combinations(range(VOTERS), QUORUM - 1))
        schedules = 0
        for prepare_a, accept_a, prepare_b, accept_b in product(quorums, first_accepts, quorums, quorums):
            with self.subTest(prepare_a=prepare_a, accept_a=accept_a, prepare_b=prepare_b, accept_b=accept_b):
                pending, _, chosen = AuthorityHistory().append(
                    Head(), "A", Presence.LIVE, 1, prepare_a, accept_a, learn=False)
                self.assertEqual(chosen, len(accept_a) >= QUORUM)
                self.assertEqual(pending.learned, Head(), "the decision notification has not arrived")
                self.assertEqual(sum(v.accepted_ballot == 1 for v in pending.slots[0]), len(accept_a))
                recovered, head, chosen = pending.append(
                    Head(1, Presence.LIVE, "A"), "B", Presence.LIVE, 2, prepare_b, accept_b)
                self.assertTrue(chosen)
                self.assertEqual(head, Head(2, Presence.LIVE, "B"))
                self.assertEqual(recovered.decisions[1].predecessor, recovered.decisions[0].successor)
                self.assertEqual({v.accepted_value for v in recovered.slots[0] if v.accepted_ballot == 2}, {0})
                schedules += 1
        print(f"multi-slot round schedules: {schedules}")

    def test_recovery_failure_or_stale_expected_head_does_not_allocate_a_successor(self):
        quorum = (0, 1, 2)
        pending, _, chosen = AuthorityHistory().append(Head(), "A", Presence.LIVE, 1, quorum, quorum, learn=False)
        self.assertTrue(chosen)
        blocked, head, chosen = pending.append(Head(1, Presence.LIVE, "A"), "B", Presence.LIVE, 2, (0, 1), quorum)
        self.assertFalse(chosen)
        self.assertIsNone(head)
        self.assertEqual(blocked.decisions, pending.decisions)
        self.assertEqual(len(blocked.slots), 1)
        recovered, _, chosen = blocked.append(Head(), "B", Presence.LIVE, 3, quorum, quorum)
        self.assertFalse(chosen, "a stale expected predecessor is rejected after recovery")
        self.assertEqual(recovered.learned, Head(1, Presence.LIVE, "A"))
        self.assertEqual(recovered.decisions, pending.decisions)
        self.assertEqual(len(recovered.slots), 1)

    def test_empty_or_unobserved_minority_slot_can_retry_without_allocating_another_revision(self):
        quorum = (0, 1, 2)
        cases = (((0, 1), quorum, quorum), (quorum, (), quorum), (quorum, (0,), (1, 2, 3)))
        for prepare_ids, accept_ids, recovery_ids in cases:
            with self.subTest(prepare=prepare_ids, accept=accept_ids, recovery=recovery_ids):
                pending, _, chosen = AuthorityHistory().append(
                    Head(), "A", Presence.LIVE, 1, prepare_ids, accept_ids)
                self.assertFalse(chosen)
                self.assertTrue(all(pending.slots[0][i].accepted_ballot == 0 for i in recovery_ids))
                retried, head, chosen = pending.append(Head(), "A", Presence.LIVE, 2, recovery_ids, recovery_ids)
                self.assertTrue(chosen, "the quorum can choose the retained candidate in the original slot")
                self.assertEqual(head, Head(1, Presence.LIVE, "A"))
                self.assertEqual(len(retried.slots), 1)
                self.assertEqual(retried.decisions, pending.decisions)
                self.assertTrue(all(retried.slots[0][i].accepted_value == 0 for i in recovery_ids))
                _, successor, chosen = retried.append(head, "B", Presence.LIVE, 3, quorum, quorum)
                self.assertTrue(chosen)
                self.assertEqual(successor.revision, 2)

    def test_superseded_operation_retry_returns_its_original_successor(self):
        quorum = (0, 1, 2)
        history, head_a, chosen = AuthorityHistory().append(Head(), "A", Presence.LIVE, 1, quorum, quorum)
        self.assertTrue(chosen)
        history, head_b, chosen = history.append(head_a, "B", Presence.LIVE, 2, quorum, quorum)
        self.assertTrue(chosen)
        retried, result, chosen = history.append(Head(), "A", Presence.LIVE, 3, quorum, quorum)
        self.assertTrue(chosen)
        self.assertEqual(result, head_a)
        self.assertEqual(retried, history, "the historical outcome cannot replace B or allocate another slot")
        self.assertEqual(retried.learned, head_b)

    def test_superseded_operation_identity_cannot_be_reused_for_different_input(self):
        quorum = (0, 1, 2)
        history, head_a, chosen = AuthorityHistory().append(Head(), "A", Presence.LIVE, 1, quorum, quorum)
        self.assertTrue(chosen)
        history, head_b, chosen = history.append(head_a, "B", Presence.LIVE, 2, quorum, quorum)
        self.assertTrue(chosen)
        for expected, presence in ((head_b, Presence.LIVE), (Head(), Presence.TOMBSTONE)):
            with self.subTest(expected=expected, presence=presence):
                rejected, result, chosen = history.append(expected, "A", presence, 3, quorum, quorum)
                self.assertFalse(chosen)
                self.assertIsNone(result)
                self.assertEqual(rejected, history)

    def test_skipping_predecessor_recovery_chooses_a_broken_revision_chain(self):
        quorum = (0, 1, 2)
        pending, _, chosen = AuthorityHistory().append(Head(), "A", Presence.LIVE, 1, quorum, quorum, learn=False)
        self.assertTrue(chosen)
        broken, head, chosen = pending.append(Head(), "B", Presence.LIVE, 2, quorum, quorum, recover_first=False)
        self.assertTrue(chosen, "the next slot alone cannot validate the predecessor")
        self.assertEqual(head.revision, 1, "the cached head caused a repeated object revision")
        self.assertNotEqual(broken.decisions[1].predecessor, broken.decisions[0].successor)
        with self.assertRaisesRegex(ValueError, "chosen predecessor"):
            replace(broken, learned=Head()).append(head, "C", Presence.LIVE, 3, quorum, quorum)

    def test_a_recovery_barrier_cannot_choose_an_empty_value(self):
        voters, value, chosen = choose((Voter(),) * VOTERS, 1, EMPTY, (0, 1, 2), (0, 1, 2))
        self.assertFalse(chosen)
        self.assertIsNone(value)
        self.assertTrue(all(v.accepted_ballot == 0 for v in voters))

    def test_tombstone_rejects_a_delayed_never_present_create(self):
        quorum = (0, 1, 2)
        history, live, chosen = AuthorityHistory().append(Head(), "create", Presence.LIVE, 1, quorum, quorum)
        self.assertTrue(chosen)
        deleted, tombstone, chosen = history.append(live, "delete", Presence.TOMBSTONE, 2, quorum, quorum)
        self.assertTrue(chosen)
        self.assertEqual(tombstone, Head(2, Presence.TOMBSTONE, "delete"))
        rejected, head, chosen = deleted.append(Head(), "stale-create", Presence.LIVE, 3, quorum, quorum)
        self.assertFalse(chosen)
        self.assertIsNone(head)
        self.assertEqual(rejected, deleted)
        recreated, head, chosen = rejected.append(tombstone, "recreate", Presence.LIVE, 4, quorum, quorum)
        self.assertTrue(chosen)
        self.assertEqual(head.revision, 3)
        self.assertEqual(recreated.decisions[2].predecessor, tombstone)
        with self.assertRaisesRegex(ValueError, "never-present"):
            recreated.append(head, "reset", Presence.NEVER_PRESENT, 5, quorum, quorum)

    def test_forgetting_tombstone_authority_allows_an_aba_create(self):
        quorum = (0, 1, 2)
        history, live, _ = AuthorityHistory().append(Head(), "create", Presence.LIVE, 1, quorum, quorum)
        deleted, tombstone, chosen = history.append(live, "delete", Presence.TOMBSTONE, 2, quorum, quorum)
        self.assertTrue(chosen)
        broken, resurrected, chosen = deleted.append(
            Head(), "stale-create", Presence.LIVE, 3, quorum, quorum, collapse_tombstone=True)
        self.assertTrue(chosen, "treating the deleted head as absent admits the delayed create")
        self.assertEqual(resurrected.revision, 1)
        self.assertNotEqual(broken.decisions[2].predecessor, tombstone)

    def test_counter_allocation_requires_persistence_and_is_consumed_once(self):
        with self.assertRaisesRegex(ValueError, "no ballot allocation"):
            ProposerSession().persist()
        reserved = ProposerSession().reserve(observed_counter=7)
        with self.assertRaisesRegex(ValueError, "already pending"):
            reserved.reserve()
        with self.assertRaisesRegex(ValueError, "persist the proposer counter"):
            reserved.issue()
        issued, ballot = reserved.persist().issue()
        self.assertEqual(ballot, 8)
        with self.assertRaisesRegex(ValueError, "no ballot allocation"):
            issued.issue()
        with self.assertRaisesRegex(ValueError, "no ballot allocation"):
            issued.restart().issue()
        _, next_ballot = issued.restart().reserve().persist().issue()
        self.assertGreater(next_ballot, ballot)

    def test_counter_restart_burns_persisted_allocations_before_or_after_issue(self):
        for crash_after in ("reserve", "persist", "issue"):
            with self.subTest(crash_after=crash_after):
                session = ProposerSession().reserve()
                if crash_after != "reserve":
                    session = session.persist()
                if crash_after == "issue":
                    session, ballot = session.issue()
                    self.assertEqual(ballot, 1)
                _, next_ballot = session.restart().reserve().persist().issue()
                self.assertEqual(next_ballot, 1 if crash_after == "reserve" else 2)

    def test_issuing_before_persistence_reuses_a_ballot_after_restart(self):
        crashed, first_ballot = ProposerSession().reserve().issue(require_persistence=False)
        quorum = (0, 1, 2)
        voters, original, chosen = choose((Voter(),) * VOTERS, first_ballot, 0, quorum, quorum)
        self.assertTrue(chosen)
        _, reused_ballot = crashed.restart().reserve().persist().issue()
        self.assertEqual(reused_ballot, first_ballot, "the emitted ballot was not reserved durably")
        self.assertEqual(voters[0].accept(reused_ballot, 1), (voters[0], False))
        _, recovered, chosen = choose(voters, reused_ballot, 1, quorum, quorum)
        self.assertTrue(chosen)
        self.assertEqual(recovered, original, "a new operation cannot use the colliding ballot")


if __name__ == "__main__":
    unittest.main(verbosity=2)
