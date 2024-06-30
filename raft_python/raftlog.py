"""
## raftlog.py

- The Raft transaction log

@author: Virat Chourasia
@version: 0.5, 10/04/2022
"""


class LogEntry:
    def __init__(self, term, value):
        self.term = term
        self.value = value

    def __repr__(self):
        return f'LogEntry({self.term}, {self.value})'

    def __eq__(self, other):
        return isinstance(other, LogEntry) and self.term == other.term and self.value == other.value

def append_entries(log, prev_index, prev_term, entries):
    '''
    Appends zero or more log entries into the log.
    prev_index specifies the position immediately before the new entries.
    prev_term specifies the term number of the log entry located at log[prev_index].
    Returns True/False depending on whether or not it worked.
    '''
    # The log is never allowed to have holes/gaps.
    if prev_index >= len(log):
        return False

    # It must be the case that log[prev_index].term == prev_term.
    # Note: Must also account for edge case of first log entry (where there is no previous entry)
    if prev_index >= 0 and log[prev_index].term != prev_term:
        return False

    # "If an existing entry conflicts with a new one (same index, but different terms),
    # delete the existing entry and all that follow it."
    for n, entry in enumerate(entries, start=prev_index+1):
        if n >= len(log):
            break
        if log[n].term != entry.term:
            del log[n:]
            break

    # "Append any new entries not already in the log"
    log[prev_index+1:prev_index+1+len(entries)] = entries
    return True

def test_append_entries():
    log = [ ]

    # Appending an initial entry should work
    assert append_entries(log, -1, -1, [ LogEntry(1, 'x') ])
    assert log == [ LogEntry(1, 'x') ], log

    # A repeated append should have no effect (should work and log is unchanged)
    assert append_entries(log, -1, -1, [ LogEntry(1, 'x') ])
    assert log == [ LogEntry(1, 'x') ], log

    # Log should not allow holes
    assert not append_entries(log, 1, 1, [ LogEntry(1, 'y') ])

    # This should work
    assert append_entries(log, 0, 1, [ LogEntry(1, 'y') ])
    assert log == [ LogEntry(1, 'x'), LogEntry(1, 'y') ], log

    # Any earlier append should "work" and leave log intact as long as the entries are the same
    # as what was there before.   Note: The entry for "y" that was added later is NOT changed!
    assert append_entries(log, -1, -1, [ LogEntry(1, 'x') ])
    assert log == [ LogEntry(1, 'x'), LogEntry(1, 'y') ], log

    # If added log entries are different (different term) that existing entries, the log is
    # deleted from that point forward.
    assert append_entries(log, -1, -1, [ LogEntry(2, 'a') ])
    assert log == [ LogEntry(2, 'a') ]

    # Adding empty log entries should work as long as prev_index and prev_term match up right
    assert append_entries(log, 0, 2, [])

    # Add multiple log entries should be ok
    assert append_entries(log, 0, 2, [ LogEntry(2, 'b'), LogEntry(2, 'c'), LogEntry(3, 'd') ])
    assert log == [ LogEntry(2, 'a'), LogEntry(2, 'b'), LogEntry(2, 'c'), LogEntry(3, 'd') ], log
    print('Good log!')

if __name__ == '__main__':
    test_append_entries()

def test_figure_7():
    # Figure 7 contains various scenarios concerning the logs on followers.
    # When the leader attempts append_entries() it will either fail or work.
    # This tests the different setups in the figure.

    def make_log(terms):
        return [ LogEntry(t, 'x') for t in terms ]

    leader_log = make_log([1,1,1,4,4,5,5,6,6,6])
    prev_index = len(leader_log) - 1
    prev_term = leader_log[prev_index].term

    assert append_entries(leader_log, prev_index, prev_term, [ LogEntry(8, 'x') ])

    # This should fail. Log has a gap at index 10
    log_a = make_log([1,1,1,4,4,5,5,6,6])
    assert not append_entries(log_a, prev_index, prev_term, [ LogEntry(8, 'x') ])

    # This should fail. Large gap
    log_b = make_log([1,1,1,4])
    assert not append_entries(log_b, prev_index, prev_term, [ LogEntry(8, 'x') ])

    # This should work. Last log entry is replaced by new one
    log_c = make_log([1,1,1,4,4,5,5,6,6,6,6])
    assert append_entries(log_c, prev_index, prev_term, [ LogEntry(8, 'x') ])
    assert log_c == leader_log

    # This should work, Last two entries are replaced by new entry
    log_d = make_log([1,1,1,4,4,5,5,6,6,6,6,7,7])
    assert append_entries(log_d, prev_index, prev_term, [ LogEntry(8, 'x') ])
    assert log_d == leader_log

    # This fails. Gap
    log_e = make_log([1,1,1,4,4,4,4])
    assert not append_entries(log_e, prev_index, prev_term, [ LogEntry(8, 'x')])

    # Fails due to log continuity (prev_term doesn't match up)
    log_f = make_log([1,1,1,2,2,2,3,3,3,3,3])
    assert not append_entries(log_f, prev_index, prev_term, [ LogEntry(8, 'x')])
    print("Good figure 7")

if __name__ == '__main__':
    test_figure_7()
