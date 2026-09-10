# Iowa batch-05 — judging notes

Sources: the enrolled act on legis.iowa.gov, which is ground truth; the Legislative Services
Agency's explanation of the introduced bill, used only for triage because it describes the
bill as introduced and is often never rewritten; and the bill history. Sponsor material was
not used. Iowa's enrolled text draws deletions and insertions as lines at fixed coordinates,
so a plain text extraction keeps both the old and the new words; every description below was
written from the resolved markup.

## SF 469 — emeritus medical license (House 58-27) — healthcare_affordability, yea for
A single new section. Physicians at least sixty years old who mainly supervise and train
residents get an emeritus license keeping their existing scope of practice.

## HF 2694 — places of worship and emergency powers (Senate 31-14) — environment_and_public_health, yea against
Section 7.19 bars the governor from closing or placing any requirement on a place or practice of
worship for any reason, naming both the disaster emergency chapter and the public health
disaster section. The act removes public health authority over a class of gatherings, which is
the direction; whether that trade is worth making is the contested part.

**This roll carries the session-end date skew.** LegiScan stamps it 2026-05-02; Iowa's journal
records the 31-14 vote on 2026-05-03, and it is one of exactly six Senate rolls in that
position. It carries an `official_vote_date` override, so the records show 2026-05-03.

## SF 2480 — nicotine and vapor product taxes (House 67-18) — environment_and_public_health, yea for
New section 453A.43A imposes five cents per container of up to twenty units of an alternative
nicotine product and five cents per milliliter of nicotine vaping solution, with the money going
to the state health care trust fund, alongside tighter licensing duties for sellers.

## HF 2539 — higher education program repeals (House 66-30, Senate 30-17) — civil_rights, yea against
Repeals the regents' minority and women educators enhancement program and its vouchers, the
college bound program and the laboratory school chapter, and strikes the regents duties and
reports tied to them. The state grant program for minority students survives, with its reference
to the repealed vouchers removed, and the description says so.
