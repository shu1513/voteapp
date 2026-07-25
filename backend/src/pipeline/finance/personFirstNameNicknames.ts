// Conservative equivalence groups for common American first-name nicknames.
// State finance sources file candidates under formal names ("Michael W
// Frerichs") while VoteApp often stores the campaign name ("Mike Frerichs"),
// so exact-key matching silently strands well-known incumbents.
//
// Resolvers must expand variants on the VoteApp side ONLY, keying
// source-side names literally. Expanding both sides would let two distinct
// formal names meet at a shared nickname key ("Patrick Smith" and "Patricia
// Smith" both produce "PAT SMITH") and attach the wrong person's data when
// only one of them exists in the source. With one-sided expansion a variant
// match still requires the same surname plus office/district/year agreement,
// and multi-candidate hits surface as ambiguous rather than linking.
//
// Keep this list to unambiguous, widely used American pairs. A nickname may
// appear in several groups ("PAT" → Patrick and Patricia); lookups return the
// union of its groups.
const FIRST_NAME_NICKNAME_GROUPS: readonly (readonly string[])[] = [
  ["ABRAHAM", "ABE"],
  ["ALBERT", "AL"],
  ["ALEXANDER", "ALEX"],
  ["ALEXANDRA", "ALEX"],
  ["ALFRED", "AL"],
  ["ANDREW", "ANDY", "DREW"],
  ["ANGELA", "ANGIE"],
  ["ANGELICA", "ANGIE"],
  ["ANTHONY", "TONY"],
  ["BARBARA", "BARB"],
  ["BENJAMIN", "BEN"],
  ["BERNARD", "BERNIE"],
  ["CALVIN", "CAL"],
  ["CHARLES", "CHARLIE", "CHUCK"],
  ["CHRISTINA", "CHRIS"],
  ["CHRISTINE", "CHRIS"],
  ["CHRISTOPHER", "CHRIS"],
  ["CYNTHIA", "CINDY"],
  ["DANIEL", "DAN", "DANNY"],
  ["DAVID", "DAVE"],
  ["DEBORAH", "DEB", "DEBBIE"],
  ["DENNIS", "DENNY"],
  ["DONALD", "DON"],
  ["DOUGLAS", "DOUG"],
  ["EDWARD", "ED", "EDDIE", "TED"],
  ["ELIZABETH", "BETH", "BETTY", "LIZ"],
  ["EUGENE", "GENE"],
  ["FRANCES", "FRAN"],
  ["FRANCIS", "FRANK"],
  ["FRANKLIN", "FRANK"],
  ["FREDERICK", "FRED"],
  ["GABRIELLA", "GABBY"],
  ["GABRIELLE", "GABBY"],
  ["GERALD", "JERRY"],
  ["GREGORY", "GREG"],
  ["HAROLD", "HARRY"],
  ["HENRY", "HANK", "HARRY"],
  ["JACOB", "JAKE"],
  ["JACQUELINE", "JACKIE"],
  ["JAMES", "JIM", "JIMMY"],
  ["JEFFERY", "JEFF"],
  ["JEFFREY", "JEFF"],
  ["JENNIFER", "JEN", "JENNY"],
  ["JEROME", "JERRY"],
  ["JESSICA", "JESS"],
  ["JOHN", "JACK", "JOHNNY"],
  ["JONATHAN", "JON"],
  ["JOSEPH", "JOE", "JOEY"],
  ["JOSHUA", "JOSH"],
  ["KATHERINE", "KATE", "KATHY", "KATIE"],
  ["KATHLEEN", "KATE", "KATHY"],
  ["KENNETH", "KEN", "KENNY"],
  ["LAWRENCE", "LARRY"],
  ["LEONARD", "LEN", "LENNY"],
  ["LOUIS", "LOU"],
  ["MARGARET", "MAGGIE", "MEG", "PEGGY"],
  ["MARTIN", "MARTY"],
  ["MATTHEW", "MATT"],
  ["MELVIN", "MEL"],
  ["MICHAEL", "MIKE"],
  ["NICHOLAS", "NICK"],
  ["PAMELA", "PAM"],
  ["PATRICIA", "PAT", "PATTY", "TRICIA"],
  ["PATRICK", "PAT"],
  ["PETER", "PETE"],
  ["RAYMOND", "RAY"],
  ["REBECCA", "BECKY"],
  ["RICHARD", "DICK", "RICH", "RICK", "RICKY"],
  ["ROBERT", "BOB", "BOBBY", "ROB"],
  ["RONALD", "RON"],
  ["RUSSELL", "RUSS"],
  ["SAMANTHA", "SAM"],
  ["SAMUEL", "SAM"],
  ["SANDRA", "SANDY"],
  ["STANLEY", "STAN"],
  ["STEPHEN", "STEVE"],
  ["STEVEN", "STEVE"],
  ["SUSAN", "SUE"],
  ["THEODORE", "TED", "TEDDY"],
  ["THOMAS", "TOM", "TOMMY"],
  ["TIMOTHY", "TIM"],
  ["VICTORIA", "VICKI", "VICKY"],
  ["VINCENT", "VINCE"],
  ["WALTER", "WALT"],
  ["WILLIAM", "BILL", "BILLY", "WILL"],
  ["ZACHARY", "ZACH"],
];

const VARIANTS_BY_NAME = new Map<string, Set<string>>();
for (const group of FIRST_NAME_NICKNAME_GROUPS) {
  for (const name of group) {
    const variants = VARIANTS_BY_NAME.get(name) ?? new Set<string>();
    for (const other of group) {
      if (other !== name) {
        variants.add(other);
      }
    }
    VARIANTS_BY_NAME.set(name, variants);
  }
}

/**
 * Returns the alternate first names equivalent to an already-normalized
 * (uppercase) first-name token, or an empty array for names with no known
 * nickname relationship.
 */
export function firstNameVariants(normalizedFirstName: string): readonly string[] {
  const variants = VARIANTS_BY_NAME.get(normalizedFirstName);
  return variants ? [...variants] : [];
}
