"""Hawaii batch-01 measure bodies. Plain English, short sentences, every statutory
qualifier kept. Imported by hi_build.py."""

MEASURES = {
  ("2175", "HB 137"): dict(
    body=("House Bill 137, which requires prison time for a felon caught with a gun or ammunition if "
          "the earlier felony was a violent crime. Under the old law this was a class B felony, but a "
          "judge could still give probation. The act makes the prison term set by state law for the "
          "offense mandatory. There is no probation or suspended sentence. It took effect when the "
          "governor signed it on April 10, 2025."),
    labels=[{"slug": "public_safety_and_crime_control", "yea": "for", "nay": None}],
    verb="passed"),
  ("2175", "SB 1433"): dict(
    body=("Senate Bill 1433, which changes Hawaii's needle exchange program. The program may now give "
          "out sterile needles and syringes based on need. Before, it could give one for each used one "
          "returned. It may also serve people who use drugs without injecting, though not with syringes. "
          "Participants cannot be charged with a drug paraphernalia offense for needles, syringes, or approved "
          "supplies such as cookers while at a program visit, and program staff cannot be charged for them "
          "while doing their jobs. Participants also cannot be charged for drug residue in a used needle or "
          "syringe for two months after their last program visit. The protection covers only "
          "exchanges between participants and program staff. Police who make an arrest in good faith are "
          "not liable."),
    labels=[{"slug": "environment_and_public_health", "yea": "for", "nay": None}],
    verb="final"),
  ("2175", "SB 897"): dict(
    body=("Senate Bill 897, which caps what an electric utility must pay for property damage after a "
          "catastrophic wildfire that its equipment may have caused or made worse. A wildfire counts as "
          "catastrophic if it substantially damages or destroys more than 500 homes or businesses, or more than 50 for an electric "
          "cooperative. The Public Utilities Commission sets the cap by rule, and the governor must approve "
          "the rule. A utility gets the cap only if it has an approved wildfire prevention plan and is "
          "carrying it out. The cap covers property losses only, not injuries or emotional harm. Utilities "
          "also stop being liable for other parties' shares of the damage. The act lets utilities sell up to "
          "$500 million in bonds for wildfire and storm-proofing work, repaid through a charge on every "
          "customer's bill. Utility executives cannot get raises unless the utility's wildfire reports are "
          "approved for five straight years."),
    labels=[{"slug": "corporate_accountability", "yea": "against", "nay": None}],
    verb="final"),
  ("2175", "SB 97"): dict(
    body=("Senate Bill 97, which makes a third excessive speeding offense within five years a misdemeanor. "
          "Excessive speeding means 30 miles per hour or more over the limit, or 80 miles per hour or more. "
          "A third offense now carries a mandatory 30 days in jail and loss of the license for 90 days to six "
          "months. The court may also order the car forfeited. Probation is not allowed. The act also lets "
          "automated speed cameras ticket any speeding violation, not only driving at least five miles per "
          "hour over the limit."),
    labels=[{"slug": "public_safety_and_crime_control", "yea": "for", "nay": None}],
    verb="final"),
  ("2245", "HB 1961"): dict(
    body=("House Bill 1961, which makes it a crime to block someone's way into or out of a health care "
          "facility. It is also a crime to tie up a facility's phone lines with repeated calls, or to "
          "threaten its patients, staff, or owners. A first offense is a petty misdemeanor with at least 24 "
          "hours in jail and a $250 fine. A third offense carries up to 30 days. Patients, staff, and "
          "facilities may also sue, and the attorney general may seek a court order. The criminal penalties "
          "apply only to adults. It says it does not limit peaceful protest or labor strikes."),
    labels=[{"slug": "public_safety_and_crime_control", "yea": "for", "nay": None}],
    verb="final"),
  ("2245", "HB 2023"): dict(
    body=("House Bill 2023, which sets up a state program for speed-limiting devices in cars. The devices "
          "warn a driver, or keep the car from going over the speed limit. The Department of Transportation "
          "will certify them and pick one vendor to install them. Starting in 2028, a driver whose license "
          "is suspended for excessive speeding may keep driving a car fitted with the device, at the "
          "driver's own cost. A judge may allow the same after a street racing conviction. Tampering with a "
          "device is a misdemeanor. Driving without a required device carries at least three days in jail."),
    labels=[{"slug": "public_safety_and_crime_control", "yea": "for", "nay": None}],
    verb="final"),
  ("2245", "SB 2239"): dict(
    body=("Senate Bill 2239, which registers people to vote automatically when they apply for a Hawaii "
          "driver's license or state ID. They can say no. Today applicants must choose to opt in. If a voter "
          "is already registered, the act updates the voter's name and address from the application unless "
          "the voter declines. The licensing office sends registration information to election officials "
          "only if the applicant shows proof of United States citizenship, or the office already has that "
          "proof on file. A non-citizen is not offered registration. It takes effect January 1, 2027."),
    labels=[{"slug": "civil_rights", "yea": "for", "nay": None},
            {"slug": "election_integrity", "yea": "for", "nay": None}],
    verb="final"),
  ("2245", "SB 2400"): dict(
    body=("Senate Bill 2400, which exempts companies that carry passengers on seagliders from Hawaii's "
          "Water Carrier Act. Seagliders are electric craft that fly just above the water between islands. "
          "Because of the exemption, the Public Utilities Commission will not regulate their fares, routes, "
          "or service. The act says the United States Coast Guard oversees their safety. Seagliders that "
          "carry cargo stay regulated."),
    labels=[{"slug": "corporate_accountability", "yea": "against", "nay": None}],
    verb="final"),
  ("2245", "SB 2694"): dict(
    body=("Senate Bill 2694, which lets regulated shipping companies raise cargo rates each year without a "
          "full rate case. The Public Utilities Commission must create an automatic adjustment tied to the "
          "state's wharfage fee increases. The adjustment is capped at 5 percent a year. The commission must "
          "apply it from July 2026 through June 2029, and may apply it through June 2033. Every third year "
          "the carrier must still file a full rate case. Customers on agricultural rates are exempt. The "
          "commission may also waive any requirement of the Water Carrier Act for a carrier. The law expires "
          "July 1, 2033."),
    labels=[{"slug": "corporate_accountability", "yea": "against", "nay": None}],
    verb="final"),
  ("2245", "SB 2697"): dict(
    body=("Senate Bill 2697, which bans driving on the shoulder of a road. The Department of Transportation "
          "may allow it at set hours. The fine is $250, or $1,000 for a repeat within five years. The act "
          "also changes when a car can be seized for unpaid registration taxes. Seizure is now allowed only "
          "when the taxes are a year or more overdue, and owners get 30 days instead of 10 to reclaim the "
          "car. Owners who bring a car from another state and do not register it face a $500 to $1,000 fine. "
          "Misusing a carpool lane now costs $250 to $1,000, up from $75 to $200."),
    labels=[{"slug": "public_safety_and_crime_control", "yea": "for", "nay": None}],
    verb="final"),
}

DROPPED = {
  ("2175", "SB 935"): ("filter 5: judges' pension formula and a vesting study; no research area covers "
                        "public-employee pension design (Alaska HB 78 precedent)"),
  ("2175", "HB 1194"): ("filter 5: permanent midwife licensing with narrower exemptions; tighter standards "
                         "against narrower access is one contested axis inside health care (Montana HB 218, "
                         "Alaska HB 173 precedent)"),
}
