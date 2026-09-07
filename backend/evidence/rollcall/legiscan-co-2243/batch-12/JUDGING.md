# Batch-12 judging notes

## LegiScan's `status` field says failed. The bill became law.

The bill record carries:

    "status": 6,            (failed)
    "status_date": "2026-05-11"

Its own action history says otherwise:

    2026-05-13 H House Third Reading Passed - No Amendments
    2026-05-13 S Senate Considered House Amendments - Result was to Concur - Repass
    2026-05-19 S Signed by the President of the Senate
    2026-05-19 H Signed by the Speaker of the House
    2026-05-19    Sent to the Governor
    2026-06-04    Governor Signed

The bill also carries Enrolled and Chaptered texts, which a failed bill does not
get. SB 80 was signed into law on 4 June 2026.

**How it was caught.** Before writing any conditional "would have" descriptions
for the not-enacted scope, every one of the 43 measures in that pool was checked
by scanning its own history for a "Governor Signed" or "Governor Vetoed" action
and comparing that against the `status` field. SB 80 was the only mismatch in
the 43. Twenty measures marked vetoed all had a matching "Governor Vetoed"
action.

**Why it matters.** Had the survey's status been trusted, SB 80's descriptions
would have said the grant program "would have" been created — telling 42
Colorado candidates they voted on something that never happened, when in fact
the program exists. That is the same class of error as trusting LegiScan's
`passed` flag against a chamber's constitutional majority, which cost HB 1187
this session and produced defects in Arizona, Indiana and Montana.

**The reusable rule: check a bill's own history for the governor's action before
believing the `status` field.**

## What the act does

Article 25 of title 26 creates the Cradle to Career Grant Program in the
department of human services. Eligible entities are local governments, local
education providers, state institutions of higher education, Indian tribes and
tribal organizations, and community-based nonprofits. Grants are for designated
service areas — geographically bounded areas whose concentration of poverty
meets the level the act specifies. Recipients may spend on family stability
services, early childhood outcomes, student achievement, transitions through
secondary and postsecondary education, workforce readiness and wealth building.
"Youth" means anyone under 25. An advisory council oversees it and a cash fund
holds the money.

## Reconciliation

42 ledger inserts = 42 rows across 42 candidates. 26 rows are yea-side and there
are exactly 26 tags. The convergence run reports all 42 `unchanged`. The dry run
wrote nothing: 5,983 records before it and 5,983 after.

No existing record was flagged as related, and nothing was retired.
