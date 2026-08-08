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

// Each summary is a newline-separated list of duty bullets. The election page
// renders them as a bulleted list under the heading
// "{canonical_name} is responsible for:", so every line must read as a
// gerund phrase completing that sentence, in plain 8th-grade language.
const SEED_OFFICES: SeedOffice[] = [
  {
    scope: "presidential",
    canonicalName: "President of the United States",
    summary: [
      "Leading the federal executive branch, its departments, and its agencies",
      "Signing bills from Congress into law, or vetoing them",
      "Serving as commander in chief of the U.S. military",
      "Conducting foreign policy, including treaties and dealings with other countries",
      "Appointing federal judges and top officials, with Senate approval",
    ].join("\n"),
  },
  {
    scope: "presidential",
    canonicalName: "Vice President of the United States",
    summary: [
      "Taking over as president if the president dies, resigns, or cannot serve",
      "Presiding over the U.S. Senate and casting tie-breaking votes",
      "Carrying out duties the president assigns, such as leading policy projects or representing the U.S. abroad",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "United States Senator",
    summary: [
      "Voting on federal laws, taxes, and the federal budget",
      "Approving or rejecting the president's nominees for judges and top officials",
      "Voting on whether to approve treaties with other countries",
      "Helping people in the state deal with federal agencies",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Governor",
    summary: [
      "Running the state government and its agencies",
      "Proposing the state budget",
      "Signing state bills into law, or vetoing them",
      "Appointing state officials and, in many states, judges",
      "Commanding the state National Guard during emergencies",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Lieutenant Governor",
    summary: [
      "Taking over as governor if the governor leaves office or cannot serve",
      "Presiding over the state senate in many states",
      "Carrying out other duties state law or the governor assigns",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Secretary of State",
    summary: [
      "Running state elections and certifying the results",
      "Keeping official state records and documents",
      "Registering businesses that operate in the state",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Attorney General",
    summary: [
      "Acting as the state's top lawyer and representing the state in court",
      "Enforcing state laws, including consumer protection and fraud cases",
      "Issuing legal opinions that guide state agencies",
      "Suing on the state's behalf, or defending the state when it is sued",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "State Treasurer",
    summary: [
      "Managing the state's bank accounts and day-to-day cash",
      "Investing state money until it is needed",
      "Managing state debt, such as bonds — money the state borrows and pays back over time",
      "Paying out state funds as the law directs",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "State Auditor",
    summary: [
      "Checking how state agencies spend public money",
      "Looking for waste, fraud, and mistakes in government programs",
      "Publishing audit reports the public can read",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Comptroller",
    summary: [
      "Keeping the state's official books and financial records",
      "Approving and tracking state payments",
      "Reporting how much money the state collects and spends",
      "Collecting certain state taxes in some states",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Commissioner of Agriculture",
    summary: [
      "Overseeing programs that support farmers and ranchers",
      "Inspecting food safety, and checking gas pumps and scales for accuracy in many states",
      "Enforcing rules on pesticides, plant health, and animal health",
      "Promoting the state's farm products",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Commissioner of Insurance",
    summary: [
      "Licensing insurance companies and agents in the state",
      "Reviewing the rates and policy terms insurers offer",
      "Investigating complaints against insurance companies",
      "Making sure insurers stay able to pay claims",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Superintendent of Public Instruction",
    summary: [
      "Leading the state education department",
      "Carrying out state rules and learning standards for public K-12 schools",
      "Distributing state money to school districts",
      "Reporting how schools and students are doing",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Public Service Commissioner",
    summary: [
      "Setting the rates utilities may charge for electricity, gas, water, or phone service",
      "Deciding whether utilities may build new plants and power lines",
      "Handling customer complaints about utility companies",
      "Making sure utility service stays safe and reliable",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Corporation Commissioner",
    summary: [
      "Regulating utility companies and their rates in states that use a corporation commission",
      "Overseeing certain other businesses, such as securities or corporations, depending on the state",
      "Handling the related licenses, filings, and complaints",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "State Level Judge",
    summary: [
      "Hearing appeals and major cases under state law",
      "Deciding what the state constitution and state laws mean",
      "Setting rulings that lower state courts must follow",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "State Board of Education Member",
    summary: [
      "Setting statewide rules and learning standards for public schools",
      "Approving graduation requirements and course standards",
      "Overseeing the state education department, and in many states the state superintendent",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "State Board of Regents Member",
    summary: [
      "Governing the state's public university system",
      "Approving university budgets and tuition rates",
      "Hiring and reviewing university presidents and chancellors",
      "Setting policies that apply across the campuses",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "State Board of Equalization Member",
    summary: [
      "Making sure property is valued evenly across counties for tax purposes",
      "Hearing taxpayers' appeals of their property assessments",
      "Handling other tax-oversight duties state law assigns",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Labor Commissioner",
    summary: [
      "Enforcing wage laws, such as minimum wage and overtime",
      "Investigating unsafe or unfair workplace practices",
      "Licensing and inspecting certain trades and workplaces",
      "Running programs that help workers and employers follow the rules",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Land Commissioner",
    summary: [
      "Managing land the state owns, including leases for grazing, oil, and gas",
      "Collecting money earned from state lands, which often funds schools",
      "Keeping official land records",
      "Protecting the natural resources on state land",
    ].join("\n"),
  },
  {
    scope: "statewide",
    canonicalName: "Railroad Commissioner",
    summary: [
      "Regulating oil and gas drilling, pipelines, mining, or utilities in states that keep a railroad commission (despite the name, the job today involves little railroad work)",
      "Issuing drilling permits and enforcing safety and environmental rules",
      "Overseeing natural gas utility rates where state law assigns it",
    ].join("\n"),
  },
  {
    scope: "us_house",
    canonicalName: "United States Representative",
    summary: [
      "Voting on federal laws, taxes, and the federal budget",
      "Representing their district's interests in Congress",
      "Starting tax and spending bills, which must begin in the House",
      "Helping district residents deal with federal agencies",
    ].join("\n"),
  },
  {
    scope: "state_upper",
    canonicalName: "State Senator",
    summary: [
      "Voting on state laws and the state budget",
      "Representing their district in the state senate",
      "Confirming certain governor appointments in many states",
      "Helping district residents deal with state agencies",
    ].join("\n"),
  },
  {
    scope: "state_lower",
    canonicalName: "State Lower Chamber Legislator",
    summary: [
      "Voting on state laws and the state budget",
      "Representing their district in the statehouse",
      "Serving on committees that shape bills before final votes",
      "Helping district residents deal with state agencies",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Commissioner",
    summary: [
      "Passing county rules and the county budget",
      "Setting property tax rates that fund county services",
      "Overseeing county departments such as roads, parks, and public health",
      "Deciding land use in areas outside city limits",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Supervisor",
    summary: [
      "Serving on the county's governing board",
      "Passing the county budget and local rules",
      "Overseeing county services such as roads, health programs, and elections",
      "Deciding land use in areas outside city limits",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Executive",
    summary: [
      "Running county government day to day, like a mayor for the county",
      "Proposing the county budget",
      "Overseeing county departments and hiring their leaders",
      "Hearing certain court cases in some Texas counties, where this office is called County Judge or County Judge/Executive",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "Borough President",
    summary: [
      "Advocating for the borough's needs inside city government",
      "Recommending how part of the city budget is spent in the borough",
      "Reviewing land-use and zoning proposals in the borough",
      "Appointing members to community boards and other bodies the city charter names",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "Sheriff",
    summary: [
      "Running the county's law-enforcement department, including deputies and patrols",
      "Running the county jail",
      "Serving court orders, such as evictions and warrants",
      "Providing courthouse security in many counties",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "District Attorney",
    summary: [
      "Deciding who gets charged with crimes in the county",
      "Prosecuting criminal cases in court on the public's behalf",
      "Deciding plea deals and what sentences to ask for",
      "Working with police as cases are investigated and built",
    ].join("\n"),
  },
  {
    // Georgia's county misdemeanor prosecutor, elected separately from the
    // District Attorney: the DA takes felonies to superior court for a
    // multi-county judicial circuit, the solicitor-general takes misdemeanors
    // to the county's State Court, and a county can hold both contests in the
    // same cycle. NOT South Carolina's "Solicitor", which IS that state's
    // felony circuit prosecutor (a District Attorney by another name).
    scope: "county",
    canonicalName: "Solicitor General",
    summary: [
      "Prosecuting misdemeanor cases in the county's state court, such as DUI, theft, and family-violence charges",
      "Deciding who gets charged with those crimes and which cases are diverted or dropped",
      "Deciding plea deals and what sentences to ask for",
      "Representing the state at misdemeanor trials and appeals",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Clerk",
    summary: [
      "Keeping official county records, such as marriage licenses and property filings",
      "Running elections in many counties",
      "Handling business filings, permits, and licenses that state law assigns",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Clerk and Recorder",
    summary: [
      "Keeping county records, including recorded property documents",
      "Running county elections and maintaining voter records",
      "Handling licenses, permits, and other filings that county law assigns",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Assessor",
    summary: [
      "Estimating what each property in the county is worth, which sets how much property tax each owner pays",
      "Keeping property maps and assessment records",
      "Handling tax exemptions, such as homeowner or veteran discounts",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Assessor-Recorder",
    summary: [
      "Estimating property values, which set how much property tax each owner pays",
      "Recording deeds, liens, and other official land documents",
      "Keeping property and ownership records open to the public",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "Public Defender",
    summary: [
      "Defending people charged with crimes who cannot afford a lawyer",
      "Running the office of defense lawyers, investigators, and support staff",
      "Making sure every defendant gets the fair defense the Constitution promises",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Auditor",
    summary: [
      "Checking county spending and financial records",
      "Watching for waste, fraud, and errors in county programs",
      "Handling payroll, payments, or election duties in some counties",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Treasurer",
    summary: [
      "Holding and managing the county's money",
      "Collecting property taxes in many counties",
      "Investing county funds until they are needed",
      "Paying out money as the county board directs",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "Public Administrator",
    summary: [
      "Settling the money and property of people who die without a will or anyone able to handle their estate, under court supervision",
      "Serving as Public Guardian in some counties — caring for adults who cannot care for themselves",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Recorder",
    summary: [
      "Recording deeds, mortgages, liens, and other official land documents",
      "Keeping those records safe and open to the public",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "Clerk of Court",
    summary: [
      "Keeping court records and case files",
      "Collecting court fines and fees",
      "Managing jury summons in many places",
      "Helping the public file and find court documents",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Coroner",
    summary: [
      "Investigating sudden, unexpected, or suspicious deaths",
      "Determining the cause of death, sometimes ordering autopsies",
      "Issuing death certificates and official findings",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Superintendent of Schools",
    summary: [
      "Supporting and overseeing local school districts at the county level",
      "Running county-wide education programs and services",
      "Reviewing district budgets or supporting teacher credentialing, depending on the state",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Level Judge",
    summary: [
      "Hearing trials — criminal cases, lawsuits, family, or probate matters, depending on the court",
      "Ruling on evidence, motions, and sentences",
      "Applying state law and court procedure fairly in every case",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "Constable",
    summary: [
      "Serving court papers, such as evictions, subpoenas, and court orders",
      "Providing security and enforcement support for local justice courts",
      "Performing limited police duties in some places",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Surveyor",
    summary: [
      "Keeping official maps and survey records of land boundaries",
      "Reviewing new subdivision maps and boundary surveys",
      "Helping settle property-line questions",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Engineer",
    summary: [
      "Planning, building, and maintaining county roads, bridges, and other public works",
      "Reviewing construction projects, permits, and engineering plans for county infrastructure",
      "Managing engineering staff, contracts, and technical records for county projects",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "Soil and Water Conservation District Supervisor",
    summary: [
      "Guiding local programs that protect soil, farmland, and water",
      "Helping landowners with drainage, erosion, and watershed projects",
      "Directing conservation funding and priorities in the district",
    ].join("\n"),
  },
  // The four offices below reached long-lived databases through a version of
  // this seed that was edited afterwards, so no current file reproduced them.
  // Definitions recovered verbatim from the local catalog.
  {
    scope: "county",
    canonicalName: "Collector of Revenue",
    summary: [
      "Collecting property, earnings, and other local taxes the county assigns",
      "Billing and receiving payment for public services and local charges",
      "Keeping records of amounts owed and paid to county government",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "License Collector",
    summary: [
      "Issuing business licenses and collecting the related local fees",
      "Keeping records of businesses authorized to operate in the county",
      "Helping businesses follow local licensing rules",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "Recorder of Deeds",
    summary: [
      "Recording deeds, mortgages, liens, and other official land documents",
      "Keeping property and ownership records safe and available to the public",
      "Providing certified copies of recorded documents when requested",
    ].join("\n"),
  },
  {
    scope: "county",
    canonicalName: "County Board of Review Member",
    summary: [
      "Hearing property-tax assessment appeals under the county's review process",
      "Reviewing whether property assessments follow state and local rules",
      "Issuing decisions that can change a property's assessed value",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Mayor",
    summary: [
      "Serving as the city's top elected leader and its public face",
      "Setting policy priorities and helping shape the city budget",
      "Running city government day to day in some cities; in others, a hired city manager runs daily operations",
      "Signing or vetoing council laws and appointing department heads, such as the police chief, in many cities",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Public Advocate",
    summary: [
      "Investigating complaints about city services",
      "Speaking for residents' concerns inside city government",
      "Performing duties the city charter assigns, such as introducing bills in the city council",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Comptroller",
    summary: [
      "Watching over the city's money and keeping its books",
      "Auditing city agencies and reviewing city contracts",
      "Overseeing city pension funds",
      "Reporting on the city's financial health",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "City Council Member",
    summary: [
      "Passing city laws, called ordinances",
      "Approving the city budget",
      "Overseeing city departments and services",
      "Representing residents' concerns at city hall",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "City Clerk",
    summary: [
      "Keeping official city records, such as meeting minutes and ordinances",
      "Publishing public notices",
      "Running local elections in some cities",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "City Treasurer",
    summary: [
      "Holding and managing the city's money",
      "Collecting certain city taxes and fees",
      "Paying city bills and keeping financial reports",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Place Level Judge",
    summary: [
      "Hearing local cases, such as traffic tickets, small claims, and city code violations",
      "Ruling on fines and penalties",
      "Applying city and state law fairly in every case",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Alderman",
    summary: [
      "Passing city laws and the city budget as a member of the city council",
      "Overseeing city services and departments",
      "Representing their ward's residents at city hall",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Town Council Member",
    summary: [
      "Passing town rules and the town budget",
      "Setting policy for town services",
      "Overseeing town departments and staff",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Municipal Trustee",
    summary: [
      "Serving on the village or town governing board",
      "Passing local rules and the budget",
      "Overseeing local services and staff",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Town Moderator",
    summary: [
      "Running town meetings, where residents debate and vote on town business",
      "Keeping debate fair and orderly, ruling on procedure, and counting votes",
      "Appointing members of certain town committees in some towns",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Municipal Assessor",
    summary: [
      "Estimating property values used for local property taxes",
      "Keeping assessment records",
      "Handling exemptions and value appeals",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Municipal Attorney",
    summary: [
      "Acting as the city's lawyer and representing it in court",
      "Advising city officials on what the law allows",
      "Drafting and reviewing ordinances and contracts",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Municipal Controller",
    summary: [
      "Keeping the city's books and financial controls",
      "Auditing city spending",
      "Preparing the city's financial reports",
    ].join("\n"),
  },
  {
    scope: "place",
    canonicalName: "Municipal Constable",
    summary: [
      "Serving court papers and local legal notices",
      "Enforcing certain local laws",
      "Performing public safety duties the town or state assigns",
    ].join("\n"),
  },
  {
    scope: "school_elementary",
    canonicalName: "School Board Member",
    summary: [
      "Setting policy for the school district",
      "Approving the district budget and spending",
      "Hiring and reviewing the superintendent",
      "Deciding school boundaries, calendars, and curriculum choices within state rules",
    ].join("\n"),
  },
  {
    scope: "school_secondary",
    canonicalName: "School Board Member",
    summary: [
      "Setting policy for the school district",
      "Approving the district budget and spending",
      "Hiring and reviewing the superintendent",
      "Deciding school boundaries, calendars, and curriculum choices within state rules",
    ].join("\n"),
  },
  {
    scope: "school_unified",
    canonicalName: "School Board Member",
    summary: [
      "Setting policy for the school district",
      "Approving the district budget and spending",
      "Hiring and reviewing the superintendent",
      "Deciding school boundaries, calendars, and curriculum choices within state rules",
    ].join("\n"),
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
    officeCanonicalName: "Borough President",
    aliasText: "Borough President",
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
    officeCanonicalName: "County Clerk and Recorder",
    aliasText: "Clerk and Recorder",
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
    officeCanonicalName: "County Engineer",
    aliasText: "County Engineer",
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
  {
    scope: "place",
    officeCanonicalName: "Public Advocate",
    aliasText: "Public Advocate",
  },
  {
    scope: "place",
    officeCanonicalName: "Comptroller",
    aliasText: "Comptroller",
  },
  {
    scope: "place",
    officeCanonicalName: "Comptroller",
    aliasText: "City Comptroller",
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
    officeCanonicalName: "City Council Member",
    aliasText: "Councilman",
  },
  {
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "Councilperson",
  },
  {
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "City Council",
  },
  {
    scope: "place",
    officeCanonicalName: "Alderman",
    aliasText: "Alderman",
  },
  {
    scope: "place",
    officeCanonicalName: "Alderman",
    aliasText: "Alderperson",
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
    officeCanonicalName: "Municipal Trustee",
    aliasText: "Trustee",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Trustee",
    aliasText: "Village Trustee",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Trustee",
    aliasText: "Town Trustee",
  },
  {
    scope: "place",
    officeCanonicalName: "Mayor",
    aliasText: "Village President",
  },
  {
    scope: "place",
    officeCanonicalName: "Mayor",
    aliasText: "Town President",
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
    officeCanonicalName: "Municipal Controller",
    aliasText: "Municipal Controller",
  },
  {
    scope: "place",
    officeCanonicalName: "Municipal Controller",
    aliasText: "City Controller",
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
  // Migration 184 batch — every alias below is a live-hit official ballot
  // title (or its jurisdiction/seat-stripped matcher key) that stranded a
  // NULL-office election shell.
  {
    // "County Council At Large" and the seat-stripped "<X> County Council"
    // forms; the catalog's existing county-council mapping ("County Council
    // Member") already points at County Supervisor.
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "County Council",
  },
  {
    // South Carolina county councils elect a chair of the legislative body;
    // this is board leadership, not the county administrator/executive.
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "County Council Chair",
  },
  {
    // Horry County SC ballots spell the same board leadership role out in
    // full. Restored from a migration that was renamed off disk before it
    // merged, so only long-lived databases carried it.
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "County Council Chairman",
  },
  {
    // Arlington County VA titles the seats on its governing legislature
    // "Member County Board" (live) — the county's legislative body, the same
    // role as the county-council aliases above, NOT the property-tax appeal
    // board. Seeded because the matcher otherwise learns this title from
    // whichever office it happens to score into first, and in the local
    // database it learned County Board of Review Member, which is wrong.
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "Member County Board",
  },
  {
    // "For Member of County Council (District 1)" (Howard County MD) after
    // the leading-"For" and seat strips.
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "Member of County Council",
  },
  {
    // "Honolulu Councilmember, Dist II" — consolidated city-county council.
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "Council Member",
  },
  {
    // DuPage County IL — presiding member of the county's legislative board.
    scope: "county",
    officeCanonicalName: "County Supervisor",
    aliasText: "County Board Chair",
  },
  {
    // Multnomah County OR — the chair runs county government (executive).
    scope: "county",
    officeCanonicalName: "County Executive",
    aliasText: "County Chair",
  },
  {
    // Maryland/Illinois-style chief county prosecutor.
    scope: "county",
    officeCanonicalName: "District Attorney",
    aliasText: "State's Attorney",
  },
  {
    // Georgia misdemeanor prosecutor ("Gwinnett County Solicitor General").
    // Held by District Attorney until migration 219 split the offices apart;
    // that migration also moves the stored alias, because upsertOfficeAlias
    // below refuses to remap an alias whose stored office disagrees with this
    // list rather than silently re-pointing it.
    scope: "county",
    officeCanonicalName: "Solicitor General",
    aliasText: "County Solicitor General",
  },
  {
    // Ballots that title the office without the county word, and the
    // hyphenated "Solicitor-General" (punctuation normalizes to a space).
    scope: "county",
    officeCanonicalName: "Solicitor General",
    aliasText: "Solicitor General",
  },
  {
    // Minnesota/Kentucky chief county prosecutor title.
    scope: "county",
    officeCanonicalName: "District Attorney",
    aliasText: "County Attorney",
  },
  {
    scope: "county",
    officeCanonicalName: "County Recorder",
    aliasText: "Register of Deeds",
  },
  {
    // "<X> COUNTY REGISTER OF DEEDS" keeps the generic civic word after the
    // jurisdiction strip (NC/TN live).
    scope: "county",
    officeCanonicalName: "County Recorder",
    aliasText: "County Register of Deeds",
  },
  {
    // New Jersey's elected county land-records officer; Hudson's official
    // title is simply "County Register" / "Register" rather than Register of
    // Deeds, but the office performs the recorder function.
    scope: "county",
    officeCanonicalName: "County Recorder",
    aliasText: "County Register",
  },
  {
    // Maryland probate registrar.
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "Register of Wills",
  },
  {
    // Massachusetts probate-court clerk.
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "Register of Probate",
  },
  {
    // "Suffolk County Register of Probate" after the jurisdiction strip.
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "County Register of Probate",
  },
  {
    // Matcher-key residue of "Register of Probate, <Name> County": the strip
    // removes the county's proper-noun core but keeps the civic word, leaving
    // it trailing (Middlesex County MA live).
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "Register of Probate County",
  },
  {
    // New Jersey probate-court officer.
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "County Surrogate",
  },
  {
    // Tennessee elected court clerks (Davidson/Shelby live).
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "County Circuit Court Clerk",
  },
  {
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "County Criminal Court Clerk",
  },
  {
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "County Juvenile Court Clerk",
  },
  {
    scope: "county",
    officeCanonicalName: "Clerk of Court",
    aliasText: "County Probate Court Clerk",
  },
  {
    // Tennessee county treasurer title ("Davidson County Trustee").
    scope: "county",
    officeCanonicalName: "County Treasurer",
    aliasText: "County Trustee",
  },
  {
    // Kentucky assessor title.
    scope: "county",
    officeCanonicalName: "County Assessor",
    aliasText: "Property Valuation Administrator",
  },
  {
    // "Jefferson County Property Valuation Administrator" after the strip.
    scope: "county",
    officeCanonicalName: "County Assessor",
    aliasText: "County Property Valuation Administrator",
  },
  {
    // Tennessee chancery-court trial judge ("Chancellor Part II ...").
    scope: "county",
    officeCanonicalName: "County Level Judge",
    aliasText: "Chancellor",
  },
  {
    // North Dakota's elected revenue commissioner; the catalog's statewide
    // fiscal-controls office is Comptroller.
    scope: "statewide",
    officeCanonicalName: "Comptroller",
    aliasText: "Tax Commissioner",
  },
  {
    // Florida-style city commissions (Deltona/Gainesville/Kissimmee/... live)
    // are the municipal council.
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "City Commission",
  },
  {
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "City Commissioner",
  },
  {
    // Carson City NV consolidated municipality's governing board.
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "City Board of Supervisors",
  },
  {
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "Board of Supervisors",
  },
  {
    // "Louisville Metro Council Member, District 23" — consolidated metro
    // council; City vs Town Council Member tie into ambiguous without it.
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "Metro Council Member",
  },
  {
    // The jurisdiction strip cannot remove "Louisville" from the title: the
    // district row is the consolidated government name ("Louisville/Jefferson
    // County metro government"), whose proper-noun core never reduces to the
    // bare city word.
    scope: "place",
    officeCanonicalName: "City Council Member",
    aliasText: "Louisville Metro Council Member",
  },
  {
    scope: "county",
    officeCanonicalName: "Constable",
    aliasText: "Constable",
  },
  {
    scope: "county",
    officeCanonicalName: "County Surveyor",
    aliasText: "County Surveyor",
  },
  {
    scope: "county",
    officeCanonicalName: "Soil and Water Conservation District Supervisor",
    aliasText: "Soil and Water Conservation District Supervisor",
  },
  {
    // "Jefferson County Soil and Water Conservation District Supervisor"
    // keeps the generic civic word after the jurisdiction strip (KY live).
    scope: "county",
    officeCanonicalName: "Soil and Water Conservation District Supervisor",
    aliasText: "County Soil and Water Conservation District Supervisor",
  },
  {
    // Horry County SC ballots use this short form; the ampersand drops out in
    // normalization ("soil water commission"). Restored from a migration that
    // was renamed off disk before it merged, so only long-lived databases
    // carried it.
    scope: "county",
    officeCanonicalName: "Soil and Water Conservation District Supervisor",
    aliasText: "Soil & Water Commission",
  },
  // Aliases for the four offices recovered above, likewise taken from the
  // local catalog rather than reconstructed.
  {
    scope: "county",
    officeCanonicalName: "Collector of Revenue",
    aliasText: "Collector of Revenue",
  },
  {
    scope: "county",
    officeCanonicalName: "License Collector",
    aliasText: "License Collector",
  },
  {
    scope: "county",
    officeCanonicalName: "Recorder of Deeds",
    aliasText: "Recorder of Deeds",
  },
  {
    scope: "county",
    officeCanonicalName: "County Board of Review Member",
    aliasText: "County Board of Review",
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
