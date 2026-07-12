import { Pool, type PoolClient } from "pg";

import { getPipelineEnv } from "../config/env.js";
import type { OfficeScope } from "../types/election.js";
import { normalizeElectionTitleKey } from "../utils/normalizeElectionTitleKey.js";

type SeedOffice = {
  scope: OfficeScope;
  canonicalName: string;
  summary: string;
};

type SeedOutcome = "inserted" | "updated" | "unchanged";

type SeedOfficeAlias = {
  scope: OfficeScope;
  officeCanonicalName: string;
  aliasText: string;
};

const SEED_OFFICES: SeedOffice[] = [
  {
    scope: "presidential",
    canonicalName: "President of the United States",
    summary:
      "Serves as head of state and head of government, leading the federal executive branch and carrying out duties assigned by the U.S. Constitution.",
  },
  {
    scope: "presidential",
    canonicalName: "Vice President of the United States",
    summary:
      "Serves as the president of the U.S. Senate, succeeds to the presidency if needed, and performs executive duties assigned by the president or law.",
  },
  {
    scope: "statewide",
    canonicalName: "United States Senator",
    summary:
      "Represents the state in the U.S. Senate, voting on federal laws, confirmations, treaties, and national policy.",
  },
  {
    scope: "statewide",
    canonicalName: "Governor",
    summary:
      "Leads the state executive branch, proposes budgets, signs or vetoes legislation, and oversees state agencies.",
  },
  {
    scope: "statewide",
    canonicalName: "Lieutenant Governor",
    summary:
      "Performs duties defined by state law, often succeeds the governor if vacant, and may have legislative or administrative responsibilities.",
  },
  {
    scope: "statewide",
    canonicalName: "Secretary of State",
    summary:
      "Administers elections and key state records, including business filings and official state documentation.",
  },
  {
    scope: "statewide",
    canonicalName: "Attorney General",
    summary:
      "Serves as the state's chief legal officer, representing the state in legal matters and enforcing state law.",
  },
  {
    scope: "statewide",
    canonicalName: "State Treasurer",
    summary:
      "Manages state funds, cash operations, investments, and debt administration under state finance rules.",
  },
  {
    scope: "statewide",
    canonicalName: "State Auditor",
    summary:
      "Audits state agencies and programs for financial accuracy, compliance, and performance accountability.",
  },
  {
    scope: "statewide",
    canonicalName: "Comptroller",
    summary:
      "Oversees statewide accounting and fiscal controls, including revenue tracking, reporting, and payments.",
  },
  {
    scope: "statewide",
    canonicalName: "Commissioner of Agriculture",
    summary:
      "Oversees state agricultural policy, industry regulation, food systems, and related inspections or programs.",
  },
  {
    scope: "statewide",
    canonicalName: "Commissioner of Insurance",
    summary:
      "Regulates insurance markets, licensing, and consumer protections for insurance products in the state.",
  },
  {
    scope: "statewide",
    canonicalName: "Superintendent of Public Instruction",
    summary:
      "Leads statewide K-12 public education administration and helps implement education policy and standards.",
  },
  {
    scope: "statewide",
    canonicalName: "Public Service Commissioner",
    summary:
      "Regulates public utilities and related services, including rates, service standards, and provider oversight.",
  },
  {
    scope: "statewide",
    canonicalName: "Corporation Commissioner",
    summary:
      "Regulates specific business sectors under state law, often including utilities, securities, or corporations.",
  },
  {
    scope: "statewide",
    canonicalName: "State Level Judge",
    summary:
      "Serves in a statewide judicial role, reviewing cases and applying state constitutional, statutory, and procedural law.",
  },
  {
    scope: "statewide",
    canonicalName: "State Board of Education Member",
    summary:
      "Sets or oversees statewide education policy, standards, and governance for the public school system.",
  },
  {
    scope: "statewide",
    canonicalName: "State Board of Regents Member",
    summary:
      "Serves on a statewide higher-education governing board overseeing public university systems, budgets, policy, and institutional governance.",
  },
  {
    scope: "statewide",
    canonicalName: "State Board of Equalization Member",
    summary:
      "Oversees state tax administration, property assessment equalization, taxpayer appeals, and related fiscal oversight duties assigned by state law.",
  },
  {
    scope: "statewide",
    canonicalName: "Labor Commissioner",
    summary:
      "Oversees labor standards, workplace protections, employment regulation, and workforce-related enforcement duties assigned by state law.",
  },
  {
    scope: "statewide",
    canonicalName: "Land Commissioner",
    summary:
      "Manages state public lands, land records, natural resource revenues, and related stewardship duties assigned by state law.",
  },
  {
    scope: "statewide",
    canonicalName: "Railroad Commissioner",
    summary:
      "Regulates rail, energy, oil and gas, pipeline, or utility sectors where those duties are assigned to a state railroad commission.",
  },
  {
    scope: "us_house",
    canonicalName: "United States Representative",
    summary:
      "Represents a congressional district in the U.S. House, voting on federal legislation and budget matters.",
  },
  {
    scope: "state_upper",
    canonicalName: "State Senator",
    summary:
      "Represents a district in the state upper legislative chamber and votes on state laws and budget policy.",
  },
  {
    scope: "state_lower",
    canonicalName: "State Lower Chamber Legislator",
    summary:
      "Represents a district in the state lower legislative chamber and votes on state laws and budget policy.",
  },
  {
    scope: "county",
    canonicalName: "County Commissioner",
    summary:
      "Sets county policy and budget priorities, oversees county services, and governs county administration.",
  },
  {
    scope: "county",
    canonicalName: "County Supervisor",
    summary:
      "Serves on the county governing board responsible for county budgets, ordinances, services, and administrative oversight.",
  },
  {
    scope: "county",
    canonicalName: "County Executive",
    summary:
      "The elected chief executive of a county government, such as a County Executive, or the presiding officer of a county commissioners court, such as a Texas County Judge or Kentucky County Judge/Executive. Responsible for county administration, budgets, and executive leadership; not a judicial office.",
  },
  {
    scope: "county",
    canonicalName: "Sheriff",
    summary:
      "Leads county law enforcement operations, jail administration, and public safety duties assigned by law.",
  },
  {
    scope: "county",
    canonicalName: "District Attorney",
    summary:
      "Serves as the county prosecutor, making charging decisions and representing the public in criminal prosecutions.",
  },
  {
    scope: "county",
    canonicalName: "County Clerk",
    summary:
      "Maintains key county records and may administer elections, filings, and licensing functions.",
  },
  {
    scope: "county",
    canonicalName: "County Assessor",
    summary:
      "Determines property valuations used to calculate local property taxes and maintains assessment records.",
  },
  {
    scope: "county",
    canonicalName: "County Assessor-Recorder",
    summary:
      "Assesses taxable property and records deeds, liens, and other official land documents when a county combines assessor and recorder duties.",
  },
  {
    scope: "county",
    canonicalName: "Public Defender",
    summary:
      "Provides constitutionally required defense representation to eligible people accused of crimes and unable to afford counsel.",
  },
  {
    scope: "county",
    canonicalName: "County Auditor",
    summary:
      "Audits county finances, oversees fiscal controls, and may administer records, payments, or elections duties assigned by state law.",
  },
  {
    scope: "county",
    canonicalName: "County Treasurer",
    summary:
      "Manages county funds, receipts, and disbursements, and may oversee tax collection or investment operations.",
  },
  {
    scope: "county",
    canonicalName: "Public Administrator",
    summary:
      "An elected county officer who administers the estates of people who die without a will or without a qualified executor, acting as a court-supervised fiduciary over decedent assets. In some counties the same officer also serves as Public Guardian for incapacitated adults.",
  },
  {
    scope: "county",
    canonicalName: "County Recorder",
    summary:
      "Records and preserves public documents such as deeds, liens, and other official county filings.",
  },
  {
    scope: "county",
    canonicalName: "Clerk of Court",
    summary:
      "Maintains court records, manages filings and case documents, and supports public access to court administration.",
  },
  {
    scope: "county",
    canonicalName: "County Coroner",
    summary:
      "Investigates certain deaths under county jurisdiction and issues findings as required by law.",
  },
  {
    scope: "county",
    canonicalName: "County Superintendent of Schools",
    summary:
      "Oversees county-level education administration and support functions for local school systems.",
  },
  {
    scope: "county",
    canonicalName: "County Level Judge",
    summary:
      "Serves in a county-level judicial role, hearing cases and issuing rulings under state and local court procedure.",
  },
  {
    scope: "place",
    canonicalName: "Mayor",
    summary:
      "Leads municipal executive functions, oversees city administration, and helps set city policy priorities.",
  },
  {
    scope: "place",
    canonicalName: "City Council Member",
    summary:
      "Serves on the city legislative body, passing ordinances, approving budgets, and overseeing city governance.",
  },
  {
    scope: "place",
    canonicalName: "City Clerk",
    summary:
      "Maintains official municipal records and may administer local elections, filings, and public notices.",
  },
  {
    scope: "place",
    canonicalName: "City Treasurer",
    summary:
      "Manages municipal financial operations including receipts, disbursements, and fiscal reporting.",
  },
  {
    scope: "place",
    canonicalName: "Place Level Judge",
    summary:
      "Serves in a municipal or place-level judicial role, handling local court matters and applying relevant law and procedure.",
  },
  {
    scope: "place",
    canonicalName: "Alderman",
    summary:
      "Serves on a municipal legislative body in jurisdictions that use alderman titles for local representatives.",
  },
  {
    scope: "place",
    canonicalName: "Town Council Member",
    summary:
      "Serves on the town legislative body, setting policy, budgets, and oversight for municipal operations.",
  },
  {
    scope: "place",
    canonicalName: "Town Moderator",
    summary:
      "Presides over town meetings or local deliberative proceedings, helping manage procedure, debate, and public votes.",
  },
  {
    scope: "place",
    canonicalName: "Municipal Assessor",
    summary:
      "Determines local property valuations used for municipal taxation and maintains assessment records.",
  },
  {
    scope: "place",
    canonicalName: "Municipal Attorney",
    summary:
      "Provides legal representation and advice for municipal government and may handle local legal matters assigned by law.",
  },
  {
    scope: "place",
    canonicalName: "Municipal Constable",
    summary:
      "Performs local law enforcement, civil process, or public safety duties assigned by municipal or state law.",
  },
  {
    scope: "school_elementary",
    canonicalName: "School Board Member",
    summary:
      "Sets school district governance policy, budgets, and superintendent oversight for local public schools.",
  },
  {
    scope: "school_secondary",
    canonicalName: "School Board Member",
    summary:
      "Sets school district governance policy, budgets, and superintendent oversight for local public schools.",
  },
  {
    scope: "school_unified",
    canonicalName: "School Board Member",
    summary:
      "Sets school district governance policy, budgets, and superintendent oversight for local public schools.",
  },
];

const SEED_OFFICE_ALIASES: SeedOfficeAlias[] = [
  {
    scope: "presidential",
    officeCanonicalName: "President of the United States",
    aliasText: "President of the United States",
  },
  {
    scope: "presidential",
    officeCanonicalName: "President of the United States",
    aliasText: "President",
  },
  {
    scope: "presidential",
    officeCanonicalName: "President of the United States",
    aliasText: "U.S. President",
  },
  {
    scope: "presidential",
    officeCanonicalName: "President of the United States",
    aliasText: "US President",
  },
  {
    scope: "presidential",
    officeCanonicalName: "Vice President of the United States",
    aliasText: "Vice President of the United States",
  },
  {
    scope: "presidential",
    officeCanonicalName: "Vice President of the United States",
    aliasText: "Vice President",
  },
  {
    scope: "presidential",
    officeCanonicalName: "Vice President of the United States",
    aliasText: "U.S. Vice President",
  },
  {
    scope: "presidential",
    officeCanonicalName: "Vice President of the United States",
    aliasText: "US Vice President",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Lieutenant Governor",
    aliasText: "Lieutenant Governor",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Lieutenant Governor",
    aliasText: "Lt. Governor",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Level Judge",
    aliasText: "State Supreme Court Justice",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Level Judge",
    aliasText: "State Court of Appeals Judge",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Level Judge",
    aliasText: "Judge",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Level Judge",
    aliasText: "Justice",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Regents Member",
    aliasText: "State Board of Regents",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Regents Member",
    aliasText: "Board of Regents",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Regents Member",
    aliasText: "Regent",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Regents Member",
    aliasText: "University Regent",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Regents Member",
    aliasText: "State University Regent",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Regents Member",
    aliasText: "Member, Board of Regents",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Regents Member",
    aliasText: "Member of the Board of Regents",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Superintendent of Public Instruction",
    aliasText: "State School Superintendent",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Superintendent of Public Instruction",
    aliasText: "State Superintendent of Public Instruction",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Superintendent of Public Instruction",
    aliasText: "State Superintendent of Education",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Equalization Member",
    aliasText: "State Board of Equalization Member",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Equalization Member",
    aliasText: "Board of Equalization Member",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Equalization Member",
    aliasText: "Member, State Board of Equalization",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Equalization Member",
    aliasText: "Member of the State Board of Equalization",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Equalization Member",
    aliasText: "Equalization Board Member",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Board of Equalization Member",
    aliasText: "State Equalization Board Member",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Labor Commissioner",
    aliasText: "Labor Commissioner",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Labor Commissioner",
    aliasText: "Commissioner of Labor",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Labor Commissioner",
    aliasText: "Commissioner of Labor and Industries",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Labor Commissioner",
    aliasText: "Labor and Industries Commissioner",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Land Commissioner",
    aliasText: "Land Commissioner",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Land Commissioner",
    aliasText: "Commissioner of Public Lands",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Land Commissioner",
    aliasText: "Commissioner of State Lands",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Land Commissioner",
    aliasText: "Commissioner of the General Land Office",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Land Commissioner",
    aliasText: "General Land Office Commissioner",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Railroad Commissioner",
    aliasText: "Railroad Commissioner",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Railroad Commissioner",
    aliasText: "Commissioner, Railroad Commission",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Railroad Commissioner",
    aliasText: "Railroad Commission Member",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Railroad Commissioner",
    aliasText: "Member, Railroad Commission",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Comptroller",
    aliasText: "State Comptroller",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Comptroller",
    aliasText: "Comptroller of Public Accounts",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Comptroller",
    aliasText: "Comptroller of the Treasury",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Comptroller",
    aliasText: "Controller",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Comptroller",
    aliasText: "State Controller",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Commissioner of Insurance",
    aliasText: "Insurance Commissioner",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Attorney General",
    aliasText: "Attorney General",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Attorney General",
    aliasText: "State Attorney General",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Attorney General",
    aliasText: "Commonwealth Attorney General",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Attorney General",
    aliasText: "Attorney General and Reporter",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Treasurer",
    aliasText: "State Treasurer",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Treasurer",
    aliasText: "Treasurer of State",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Treasurer",
    aliasText: "Commonwealth Treasurer",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Treasurer",
    aliasText: "Treasurer",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Auditor",
    aliasText: "State Auditor",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Auditor",
    aliasText: "Auditor of State",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Auditor",
    aliasText: "Auditor of Public Accounts",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Auditor",
    aliasText: "State Auditor and Inspector",
  },
  {
    scope: "statewide",
    officeCanonicalName: "State Auditor",
    aliasText: "State Auditor of Accounts",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Secretary of State",
    aliasText: "Secretary of State",
  },
  {
    scope: "statewide",
    officeCanonicalName: "Secretary of State",
    aliasText: "Secretary of the Commonwealth",
  },
  {
    scope: "state_upper",
    officeCanonicalName: "State Senator",
    aliasText: "Member of the Legislature",
  },
  {
    scope: "state_upper",
    officeCanonicalName: "State Senator",
    aliasText: "Member of the State Senate",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "House Delegate",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "State Assembly Member",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "State Representative",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "Representative in the General Assembly",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "Representative in General Court",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "State Delegate",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "Member, House of Delegates",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "Member of the Assembly",
  },
  {
    scope: "state_lower",
    officeCanonicalName: "State Lower Chamber Legislator",
    aliasText: "Member of the State Assembly",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "United States Representative in Congress",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "U.S. Representative in Congress",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "Representative in Congress",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "United States Representative",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "Member, House of Representatives",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "House of Representatives",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "U.S. House of Representatives",
  },
  {
    scope: "us_house",
    officeCanonicalName: "United States Representative",
    aliasText: "US House of Representatives",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "County Supervisor",
  },
  {
    scope: "county",
    officeCanonicalName: "County Executive",
    aliasText: "County Executive",
  },
  {
    scope: "county",
    officeCanonicalName: "County Executive",
    aliasText: "County Judge",
  },
  {
    scope: "county",
    officeCanonicalName: "County Executive",
    aliasText: "County Judge/Executive",
  },
  {
    scope: "county",
    officeCanonicalName: "County Executive",
    aliasText: "County Mayor",
  },
  {
    scope: "county",
    officeCanonicalName: "Public Administrator",
    aliasText: "Public Administrator",
  },
  {
    scope: "county",
    officeCanonicalName: "Public Administrator",
    aliasText: "Public Administrator/Public Guardian",
  },
  {
    scope: "county",
    officeCanonicalName: "Public Administrator",
    aliasText: "Public Administrator/Guardian",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "Supervisor",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "Member, Board of Supervisors",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "Board of Supervisors Member",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "Member of the Board of Supervisors",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "Board of Supervisors",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "County Board Supervisor",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "County Council Member",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "County Board Member",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "County Legislator",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "Parish Police Juror",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "Fiscal Court Member",
  },
  {
    scope: "county",
    officeCanonicalName: "County Assessor-Recorder",
    aliasText: "Assessor-Recorder",
  },
  {
    scope: "county",
    officeCanonicalName: "County Assessor-Recorder",
    aliasText: "County Assessor-Recorder",
  },
  {
    scope: "county",
    officeCanonicalName: "Public Defender",
    aliasText: "Public Defender",
  },
  {
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "Quorum Court Member",
  },
  {
    scope: "county",
    officeCanonicalName: "District Attorney",
    aliasText: "District Attorney",
  },
  {
    scope: "county",
    officeCanonicalName: "District Attorney",
    aliasText: "County District Attorney",
  },
  {
    scope: "county",
    officeCanonicalName: "District Attorney",
    aliasText: "Prosecuting Attorney",
  },
  {
    scope: "county",
    officeCanonicalName: "District Attorney",
    aliasText: "County Prosecutor",
  },
  {
    scope: "county",
    officeCanonicalName: "County Auditor",
    aliasText: "County Auditor",
  },
  {
    scope: "county",
    officeCanonicalName: "County Auditor",
    aliasText: "Auditor",
  },
  {
    scope: "county",
    officeCanonicalName: "County Auditor",
    aliasText: "County Controller",
  },
  {
    scope: "county",
    officeCanonicalName: "County Auditor",
    aliasText: "County Comptroller",
  },
  {
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "Clerk of Court",
  },
  {
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "Clerk of the Court",
  },
  {
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "Circuit Clerk",
  },
  {
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "Court Clerk",
  },
  {
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "County Court Clerk",
  },
  {
    scope: "county",
    officeCanonicalName: "County Level Judge",
    aliasText: "Superior Court Judge",
  },
  {
    scope: "county",
    officeCanonicalName: "County Level Judge",
    aliasText: "Probate Judge",
  },
  {
    scope: "county",
    officeCanonicalName: "County Level Judge",
    aliasText: "Judge",
  },
  // New York elects Supreme Court Justices by numbered Judicial District
  // (1st-13th); each maps to the generic county judge office.
  ...["1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th", "11th", "12th", "13th"].map(
    (ordinal) => ({
      scope: "county" as const,
      officeCanonicalName: "County Level Judge",
      aliasText: `Supreme Court Justice - ${ordinal} Judicial District`,
    })
  ),
  {
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "City Council Member",
  },
  // El Paso titles its council members "City Representative"; the matcher's
  // jurisdiction/seat stripping reduces those titles to this generic form.
  {
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "City Representative",
  },
  {
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "Council Member",
  },
  {
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "City Councilor",
  },
  {
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "Municipal Council Member",
  },
  {
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "Municipal Governing Board Member",
  },
  {
    scope: "place",
    officeCanonicalName: "Town Council Member",
    aliasText: "Town Council Member",
  },
  {
    scope: "place",
    officeCanonicalName: "Town Council Member",
    aliasText: "Town Board Member",
  },
  {
    scope: "place",
    officeCanonicalName: "Town Council Member",
    aliasText: "Select Board Member",
  },
  {
    scope: "place",
    officeCanonicalName: "Town Council Member",
    aliasText: "Town Select Board Member",
  },
  {
    scope: "place",
    officeCanonicalName: "City Clerk",
    aliasText: "City Clerk",
  },
  {
    scope: "place",
    officeCanonicalName: "City Clerk",
    aliasText: "Municipal Clerk",
  },
  {
    scope: "place",
    officeCanonicalName: "City Clerk",
    aliasText: "Town Clerk",
  },
  {
    scope: "place",
    officeCanonicalName: "City Clerk",
    aliasText: "Village Clerk",
  },
  {
    scope: "place",
    officeCanonicalName: "City Treasurer",
    aliasText: "City Treasurer",
  },
  {
    scope: "place",
    officeCanonicalName: "City Treasurer",
    aliasText: "Municipal Treasurer",
  },
  {
    scope: "place",
    officeCanonicalName: "City Treasurer",
    aliasText: "Town Treasurer",
  },
  {
    scope: "place",
    officeCanonicalName: "City Treasurer",
    aliasText: "Village Treasurer",
  },
  {
    scope: "place",
    officeCanonicalName: "Town Moderator",
    aliasText: "Town Moderator",
  },
  {
    scope: "place",
    officeCanonicalName: "Town Moderator",
    aliasText: "Moderator",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Assessor",
    aliasText: "Municipal Assessor",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Assessor",
    aliasText: "City Assessor",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Assessor",
    aliasText: "Town Assessor",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Assessor",
    aliasText: "Village Assessor",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Attorney",
    aliasText: "Municipal Attorney",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Attorney",
    aliasText: "City Attorney",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Attorney",
    aliasText: "Town Attorney",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Attorney",
    aliasText: "Village Attorney",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Constable",
    aliasText: "Municipal Constable",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Constable",
    aliasText: "City Constable",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Constable",
    aliasText: "Town Constable",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Constable",
    aliasText: "Village Constable",
  },
  {
    scope: "place",
    officeCanonicalName: "Place Level Judge",
    aliasText: "Municipal Judge",
  },
  {
    scope: "place",
    officeCanonicalName: "Place Level Judge",
    aliasText: "Judge",
  },
];

function assertNoDuplicateSeedKeys(rows: readonly SeedOffice[]): void {
  const seen = new Set<string>();
  for (const row of rows) {
    const key = `${row.scope}::${row.canonicalName.trim().toLowerCase()}`;
    if (seen.has(key)) {
      throw new Error(`Duplicate office seed key: ${key}`);
    }
    seen.add(key);
  }
}

function assertNoDuplicateSeedAliasKeys(rows: readonly SeedOfficeAlias[]): void {
  const seen = new Map<string, string>();
  for (const row of rows) {
    const normalizedAlias = normalizeElectionTitleKey(row.aliasText);
    const key = `${row.scope}::${normalizedAlias}`;
    const owner = `${row.scope}::${row.officeCanonicalName}`;
    const existingOwner = seen.get(key);
    if (existingOwner) {
      throw new Error(
        `Duplicate office alias seed key: ${key} claimed by ${existingOwner} and ${owner}`
      );
    }
    seen.set(key, owner);
  }
}

async function upsertOffice(client: PoolClient, row: SeedOffice): Promise<SeedOutcome> {
  const updated = await client.query<{ id: string }>(
    `
      UPDATE public.offices
      SET summary = $3,
          updated_at = now()
      WHERE scope = $1
        AND canonical_name = $2
        AND summary IS DISTINCT FROM $3
      RETURNING id
    `,
    [row.scope, row.canonicalName, row.summary]
  );
  if (updated.rowCount === 1) {
    return "updated";
  }

  const inserted = await client.query<{ id: string }>(
    `
      INSERT INTO public.offices (scope, canonical_name, summary)
      VALUES ($1, $2, $3)
      ON CONFLICT (scope, canonical_name) DO NOTHING
      RETURNING id
    `,
    [row.scope, row.canonicalName, row.summary]
  );
  if (inserted.rowCount === 1) {
    return "inserted";
  }

  return "unchanged";
}

async function resolveOfficeIdByScopeAndName(
  client: PoolClient,
  row: SeedOfficeAlias
): Promise<string> {
  const result = await client.query<{ id: string }>(
    `
      SELECT id
      FROM public.offices
      WHERE scope = $1
        AND canonical_name = $2
      LIMIT 1
    `,
    [row.scope, row.officeCanonicalName]
  );

  const officeId = result.rows?.[0]?.id;
  if (!officeId) {
    throw new Error(
      `Missing canonical office for alias seed: scope=${row.scope} canonical_name=${row.officeCanonicalName}`
    );
  }
  return officeId;
}

async function upsertOfficeAlias(client: PoolClient, row: SeedOfficeAlias): Promise<SeedOutcome> {
  const officeId = await resolveOfficeIdByScopeAndName(client, row);
  const normalizedAlias = normalizeElectionTitleKey(row.aliasText);
  if (!normalizedAlias) {
    throw new Error(
      `Alias seed normalized to empty key: scope=${row.scope} alias_text=${JSON.stringify(row.aliasText)}`
    );
  }

  const existing = await client.query<{
    id: string;
    office_id: string;
    alias_text: string;
    canonical_name: string;
  }>(
    `
      SELECT alias.id,
             alias.office_id,
             alias.alias_text,
             office.canonical_name
      FROM public.office_title_aliases alias
      JOIN public.offices office
        ON office.id = alias.office_id
      WHERE alias.scope = $1
        AND alias.normalized_alias = $2
      LIMIT 1
    `,
    [row.scope, normalizedAlias]
  );

  const existingAlias = existing.rows[0];
  if (existingAlias && existingAlias.office_id !== officeId) {
    throw new Error(
      `Office alias collision: scope=${row.scope} normalized_alias=${normalizedAlias} ` +
        `already maps to ${existingAlias.canonical_name}; refused to remap to ${row.officeCanonicalName}`
    );
  }

  if (existingAlias) {
    const updated = await client.query<{ id: string }>(
      `
        UPDATE public.office_title_aliases
        SET alias_text = $3,
            updated_at = now()
        WHERE id = $1
          AND office_id = $2
          AND alias_text IS DISTINCT FROM $3
        RETURNING id
      `,
      [existingAlias.id, officeId, row.aliasText]
    );
    if (updated.rowCount === 1) {
      return "updated";
    }

    const verified = await client.query<{ office_id: string; canonical_name: string }>(
      `
        SELECT alias.office_id, office.canonical_name
        FROM public.office_title_aliases alias
        JOIN public.offices office
          ON office.id = alias.office_id
        WHERE alias.id = $1
        LIMIT 1
      `,
      [existingAlias.id]
    );
    const verifiedAlias = verified.rows[0];
    if (!verifiedAlias) {
      throw new Error(
        `Office alias disappeared during seed: scope=${row.scope} normalized_alias=${normalizedAlias}`
      );
    }
    if (verifiedAlias.office_id !== officeId) {
      throw new Error(
        `Office alias collision during update: scope=${row.scope} normalized_alias=${normalizedAlias} ` +
          `maps to ${verifiedAlias.canonical_name}; refused to remap to ${row.officeCanonicalName}`
      );
    }
    return "unchanged";
  }

  const inserted = await client.query<{ id: string }>(
    `
      INSERT INTO public.office_title_aliases (
        office_id,
        scope,
        alias_text,
        normalized_alias
      )
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (scope, normalized_alias) DO NOTHING
      RETURNING id
    `,
    [officeId, row.scope, row.aliasText, normalizedAlias]
  );
  if (inserted.rowCount === 1) {
    return "inserted";
  }

  const raced = await client.query<{
    id: string;
    office_id: string;
    alias_text: string;
    canonical_name: string;
  }>(
    `
      SELECT alias.id,
             alias.office_id,
             alias.alias_text,
             office.canonical_name
      FROM public.office_title_aliases alias
      JOIN public.offices office
        ON office.id = alias.office_id
      WHERE alias.scope = $1
        AND alias.normalized_alias = $2
      LIMIT 1
    `,
    [row.scope, normalizedAlias]
  );

  const racedAlias = raced.rows[0];
  if (racedAlias && racedAlias.office_id !== officeId) {
    throw new Error(
      `Office alias collision after insert race: scope=${row.scope} normalized_alias=${normalizedAlias} ` +
        `maps to ${racedAlias.canonical_name}; refused to remap to ${row.officeCanonicalName}`
    );
  }

  if (racedAlias) {
    const updated = await client.query<{ id: string }>(
      `
        UPDATE public.office_title_aliases
        SET alias_text = $3,
            updated_at = now()
        WHERE id = $1
          AND office_id = $2
          AND alias_text IS DISTINCT FROM $3
        RETURNING id
      `,
      [racedAlias.id, officeId, row.aliasText]
    );
    if (updated.rowCount === 1) {
      return "updated";
    }

    const verified = await client.query<{ office_id: string; canonical_name: string }>(
      `
        SELECT alias.office_id, office.canonical_name
        FROM public.office_title_aliases alias
        JOIN public.offices office
          ON office.id = alias.office_id
        WHERE alias.id = $1
        LIMIT 1
      `,
      [racedAlias.id]
    );
    const verifiedAlias = verified.rows[0];
    if (!verifiedAlias) {
      throw new Error(
        `Office alias disappeared after insert race: scope=${row.scope} normalized_alias=${normalizedAlias}`
      );
    }
    if (verifiedAlias.office_id !== officeId) {
      throw new Error(
        `Office alias collision after insert race update: scope=${row.scope} normalized_alias=${normalizedAlias} ` +
          `maps to ${verifiedAlias.canonical_name}; refused to remap to ${row.officeCanonicalName}`
      );
    }
    return "unchanged";
  }

  return "unchanged";
}

async function main(): Promise<void> {
  assertNoDuplicateSeedKeys(SEED_OFFICES);
  assertNoDuplicateSeedAliasKeys(SEED_OFFICE_ALIASES);

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const startedAt = new Date();

  const outcomeCounts: Record<SeedOutcome, number> = {
    inserted: 0,
    updated: 0,
    unchanged: 0,
  };
  const scopeCounts = new Map<OfficeScope, number>();
  const aliasOutcomeCounts: Record<SeedOutcome, number> = {
    inserted: 0,
    updated: 0,
    unchanged: 0,
  };
  const aliasScopeCounts = new Map<OfficeScope, number>();

  let client: PoolClient | undefined;
  try {
    client = await pool.connect();
    await client.query("BEGIN");
    for (const row of SEED_OFFICES) {
      const outcome = await upsertOffice(client, row);
      outcomeCounts[outcome] += 1;
      scopeCounts.set(row.scope, (scopeCounts.get(row.scope) ?? 0) + 1);
    }
    for (const row of SEED_OFFICE_ALIASES) {
      const outcome = await upsertOfficeAlias(client, row);
      aliasOutcomeCounts[outcome] += 1;
      aliasScopeCounts.set(row.scope, (aliasScopeCounts.get(row.scope) ?? 0) + 1);
    }
    await client.query("COMMIT");
  } catch (error) {
    if (client) {
      await client.query("ROLLBACK");
    }
    throw error;
  } finally {
    client?.release();
    await pool.end();
  }

  const output = {
    type: "offices_seed",
    ts: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    total_seed_rows: SEED_OFFICES.length,
    outcomes: outcomeCounts,
    by_scope: Array.from(scopeCounts.entries()).map(([scope, count]) => ({ scope, count })),
    alias_seed_rows: SEED_OFFICE_ALIASES.length,
    alias_outcomes: aliasOutcomeCounts,
    alias_by_scope: Array.from(aliasScopeCounts.entries()).map(([scope, count]) => ({ scope, count })),
  };

  console.log(JSON.stringify(output, null, 2));
}

main().catch((error) => {
  console.error("offices seed failed:", error);
  process.exit(1);
});
