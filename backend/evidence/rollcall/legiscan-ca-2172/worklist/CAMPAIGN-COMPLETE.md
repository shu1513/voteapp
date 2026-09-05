# California session 2172 — the ranked pool is fully worked

Every measure in `worklist.json` has a terminal disposition in `ledger.json`. Nothing is open.

| | count |
| --- | --- |
| live measures in the ranked pool | 244 |
| judged and imported | 136 |
| read and dropped, with a reason recorded | 108 |

## Why 108 measures were dropped

A drop is a result, not a gap. Each one records why, so no later batch re-reads the same bill and
reaches a different answer.

| reason | count |
| --- | --- |
| cuts both ways on one dimension | 41 |
| budget trailer or omnibus vehicle | 18 |
| local or procedural only | 10 |
| no research area fits | 10 |
| no real operative duty | 8 |
| joint resolution, changes no law | 5 |
| other recorded reasons | 16 |

**The single largest cause is bills that tighten and loosen the same thing.** That test came from
the AB 1078 retraction: a stance label is machine-readable and drives candidate sorting, so a bill
pushing both ways on one dimension gets no label, however well its description explains itself.

**The second is budget trailer bills** authored by a budget committee. They bundle unrelated policy
that points several ways at once, and a vote on one is a vote on a budget package rather than on
any provision inside it.

**Ten drops are a gap in the tooling, not the bills.** Most are labor measures — a union scoring
preference, notice before contracting out, a civil service preference for prison clinicians,
worker rights outreach. Each is clean and one-directional. The research area list has no entry for
labor or union rights, and forcing an existing area onto them would tell a voter something the vote
does not support. **Adding one area would let those votes be recorded.**

## Rules this campaign settled

- Mixed direction on one dimension means no label. A limit, carve-out, delay or scope exception is
  described instead.
- Area descriptions govern tagging, read from the database rather than from memory. `election_integrity`
  is about elections being secure and auditable, not accessible; access bills go to `civil_rights`.
- A single-county bill is kept when it makes a substantive policy choice, and the description names
  the county. It is dropped when it only decides how or when a local measure reaches the ballot.
- A mandatory duty is judgeable even when weak. A purely permissive one is not.
- A contingent duty is judgeable when a real party is bound once funded, and not when the bill ends
  in recommendations.
- A measure read and dropped must be recorded, because the pool script infers "worked" from
  judgment files and a drop produces none. Before a batch, search earlier batch documents too.

## Owed after 2026-09-30

Roughly 90 enrolled measures across batches 11-24 are written in the conditional and need a real
rewrite once the governor signs or vetoes them. The trigger is the enrolled count falling in a
later dataset cut, not a refresh.
