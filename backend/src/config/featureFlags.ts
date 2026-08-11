function readBooleanEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) {
    return fallback;
  }

  const normalized = raw.trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(normalized)) {
    return false;
  }

  throw new Error(`Invalid boolean env ${name}: ${raw}`);
}

export function isPresidentialElectionsEnabled(): boolean {
  return readBooleanEnv("PRESIDENTIAL_ELECTIONS_ENABLED", true);
}

export function isPresidentialFeatureEnabled(featureEnvName: string, force = false): boolean {
  return isPresidentialElectionsEnabled() && (force || readBooleanEnv(featureEnvName, false));
}

export function isAutoDistrictResearchEnabled(): boolean {
  return readBooleanEnv("AUTO_DISTRICT_RESEARCH_ENABLED", false);
}

export type AutoDistrictResearchMode = "off" | "ai" | "manual";

/**
 * Which auto district research behavior fires when an address lookup resolves
 * stale districts. Exactly one is active so the AI pipeline and the manual
 * agent queue never both enqueue the same district.
 *
 *   - "ai": drop an election draft into the staging pipeline (PR #169).
 *   - "manual": enqueue a manual research request for an agent to claim.
 *   - "off": no auto research.
 *
 * AUTO_DISTRICT_RESEARCH_MODE is authoritative. When it is unset, the legacy
 * boolean AUTO_DISTRICT_RESEARCH_ENABLED=true maps to "ai" for backward
 * compatibility; otherwise the default is "manual" — enqueueing is
 * Postgres-only, deduped, and fire-and-forget, so it is safe on by default
 * (even against a database missing the queue table, the enqueue just warns).
 * Turn the feature off with AUTO_DISTRICT_RESEARCH_MODE=off in backend/.env.
 * Setting a mode that conflicts with the legacy boolean is an error so a
 * stale boolean cannot silently override an explicit mode.
 */
export function readAutoDistrictResearchMode(): AutoDistrictResearchMode {
  const raw = process.env.AUTO_DISTRICT_RESEARCH_MODE;
  const legacyEnabled = isAutoDistrictResearchEnabled();

  if (raw && raw.trim().length > 0) {
    const normalized = raw.trim().toLowerCase();
    if (normalized !== "off" && normalized !== "ai" && normalized !== "manual") {
      throw new Error(`Invalid AUTO_DISTRICT_RESEARCH_MODE: ${raw}. Expected one of off, ai, manual.`);
    }
    if (legacyEnabled && normalized !== "ai") {
      throw new Error(
        `Conflicting config: AUTO_DISTRICT_RESEARCH_MODE=${normalized} with AUTO_DISTRICT_RESEARCH_ENABLED=true. ` +
          "Set only AUTO_DISTRICT_RESEARCH_MODE."
      );
    }
    return normalized;
  }

  return legacyEnabled ? "ai" : "manual";
}

export function isCandidateFinanceEnabled(): boolean {
  return readBooleanEnv("CANDIDATE_FINANCE_ENABLED", false);
}

export function isCandidateFinanceSyncEnabled(force = false): boolean {
  return isCandidateFinanceEnabled() && (force || readBooleanEnv("CANDIDATE_FINANCE_SYNC_ENABLED", false));
}

export function isArizonaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("ARIZONA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isArizonaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isArizonaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("ARIZONA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isCaliforniaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("CALIFORNIA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isCaliforniaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isCaliforniaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("CALIFORNIA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isCaliforniaCampaignFinanceRawDataRefreshEnabled(force = false): boolean {
  return (
    isCaliforniaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isAlaskaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("ALASKA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isAlaskaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isAlaskaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("ALASKA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isColoradoCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("COLORADO_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isColoradoCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isColoradoCampaignFinanceEnabled() &&
    (force || readBooleanEnv("COLORADO_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isColoradoTracerRawDataRefreshEnabled(force = false): boolean {
  return (
    isColoradoCampaignFinanceEnabled() &&
    (force || readBooleanEnv("COLORADO_TRACER_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isConnecticutCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("CONNECTICUT_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isConnecticutCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isConnecticutCampaignFinanceEnabled() &&
    (force || readBooleanEnv("CONNECTICUT_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isConnecticutEcrisRawDataRefreshEnabled(force = false): boolean {
  return (
    isConnecticutCampaignFinanceEnabled() &&
    (force || readBooleanEnv("CONNECTICUT_ECRIS_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isNebraskaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("NEBRASKA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isNebraskaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isNebraskaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("NEBRASKA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isNebraskaNadcRawDataRefreshEnabled(force = false): boolean {
  return (
    isNebraskaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("NEBRASKA_NADC_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isIndianaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("INDIANA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isIndianaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isIndianaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isIndianaCampaignFinanceRawDataRefreshEnabled(force = false): boolean {
  return (
    isIndianaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isNewMexicoCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isNewMexicoCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isNewMexicoCampaignFinanceEnabled() &&
    (force || readBooleanEnv("NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isNewJerseyCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("NEW_JERSEY_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isNewJerseyCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isNewJerseyCampaignFinanceEnabled() &&
    (force || readBooleanEnv("NEW_JERSEY_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isNewMexicoCfisRawDataRefreshEnabled(force = false): boolean {
  return (
    isNewMexicoCampaignFinanceEnabled() &&
    (force || readBooleanEnv("NEW_MEXICO_CFIS_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isMarylandCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("MARYLAND_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isMarylandCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isMarylandCampaignFinanceEnabled() &&
    (force || readBooleanEnv("MARYLAND_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isMarylandCfsRawDataRefreshEnabled(force = false): boolean {
  return (
    isMarylandCampaignFinanceEnabled() &&
    (force || readBooleanEnv("MARYLAND_CFS_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isOhioCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("OHIO_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isOhioCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isOhioCampaignFinanceEnabled() &&
    (force || readBooleanEnv("OHIO_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isOhioSosRawDataRefreshEnabled(force = false): boolean {
  return (
    isOhioCampaignFinanceEnabled() &&
    (force || readBooleanEnv("OHIO_SOS_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isNorthCarolinaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("NORTH_CAROLINA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isNorthCarolinaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isNorthCarolinaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("NORTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isNorthCarolinaNcsbeRawDataRefreshEnabled(force = false): boolean {
  return (
    isNorthCarolinaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("NORTH_CAROLINA_NCSBE_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isGeorgiaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("GEORGIA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isGeorgiaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isGeorgiaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("GEORGIA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isGeorgiaEthicsRawDataRefreshEnabled(force = false): boolean {
  return (
    isGeorgiaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("GEORGIA_ETHICS_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isMaineCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("MAINE_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isMaineCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isMaineCampaignFinanceEnabled() &&
    (force || readBooleanEnv("MAINE_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isMaineCfisRawDataRefreshEnabled(force = false): boolean {
  return (
    isMaineCampaignFinanceEnabled() &&
    (force || readBooleanEnv("MAINE_CFIS_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isOklahomaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("OKLAHOMA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isOklahomaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isOklahomaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("OKLAHOMA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isOklahomaGuardianRawDataRefreshEnabled(force = false): boolean {
  return (
    isOklahomaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("OKLAHOMA_GUARDIAN_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isTexasCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("TEXAS_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isTexasCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isTexasCampaignFinanceEnabled() &&
    (force || readBooleanEnv("TEXAS_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isHoustonCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("HOUSTON_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isHoustonCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isHoustonCampaignFinanceEnabled() &&
    (force || readBooleanEnv("HOUSTON_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isTennesseeCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("TENNESSEE_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isTennesseeCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isTennesseeCampaignFinanceEnabled() &&
    (force || readBooleanEnv("TENNESSEE_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isTexasTecRawDataRefreshEnabled(force = false): boolean {
  return (
    isTexasCampaignFinanceEnabled() &&
    (force || readBooleanEnv("TEXAS_TEC_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isFloridaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("FLORIDA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isFloridaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isFloridaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("FLORIDA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isFloridaCampaignFinanceBrowserExportEnabled(force = false): boolean {
  return (
    isFloridaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("FLORIDA_CAMPAIGN_FINANCE_BROWSER_EXPORT_ENABLED", false))
  );
}

export function isWashingtonCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("WASHINGTON_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isWashingtonCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isWashingtonCampaignFinanceEnabled() &&
    (force || readBooleanEnv("WASHINGTON_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isNewYorkCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("NEW_YORK_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isNewYorkCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isNewYorkCampaignFinanceEnabled() &&
    (force || readBooleanEnv("NEW_YORK_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isNewYorkCityCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("NEW_YORK_CITY_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isNewYorkCityCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isNewYorkCityCampaignFinanceEnabled() &&
    (force || readBooleanEnv("NEW_YORK_CITY_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isLosAngelesCityCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("LOS_ANGELES_CITY_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isLosAngelesCityCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isLosAngelesCityCampaignFinanceEnabled() &&
    (force || readBooleanEnv("LOS_ANGELES_CITY_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isSanFranciscoCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("SAN_FRANCISCO_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isSanFranciscoCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isSanFranciscoCampaignFinanceEnabled() &&
    (force || readBooleanEnv("SAN_FRANCISCO_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isSanJoseCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("SAN_JOSE_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isSanJoseCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isSanJoseCampaignFinanceEnabled() &&
    (force || readBooleanEnv("SAN_JOSE_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isSanJoseCampaignFinanceRawDataRefreshEnabled(force = false): boolean {
  return (
    isSanJoseCampaignFinanceEnabled() &&
    (force || readBooleanEnv("SAN_JOSE_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isHawaiiCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("HAWAII_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isHawaiiCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isHawaiiCampaignFinanceEnabled() &&
    (force || readBooleanEnv("HAWAII_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isDistrictOfColumbiaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isDistrictOfColumbiaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isDistrictOfColumbiaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isIllinoisCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("ILLINOIS_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isIllinoisCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isIllinoisCampaignFinanceEnabled() &&
    (force || readBooleanEnv("ILLINOIS_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isKentuckyCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("KENTUCKY_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isKentuckyCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isKentuckyCampaignFinanceEnabled() &&
    (force || readBooleanEnv("KENTUCKY_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isVirginiaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("VIRGINIA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isVirginiaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isVirginiaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("VIRGINIA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isWisconsinCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("WISCONSIN_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isWisconsinCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isWisconsinCampaignFinanceEnabled() &&
    (force || readBooleanEnv("WISCONSIN_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isUtahCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("UTAH_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isUtahCampaignFinanceSyncEnabled(force = false): boolean {
  return isUtahCampaignFinanceEnabled() && (force || readBooleanEnv("UTAH_CAMPAIGN_FINANCE_SYNC_ENABLED", false));
}

export function isMassachusettsCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("MASSACHUSETTS_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isMassachusettsCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isMassachusettsCampaignFinanceEnabled() &&
    (force || readBooleanEnv("MASSACHUSETTS_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isVermontCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("VERMONT_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isVermontCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isVermontCampaignFinanceEnabled() &&
    (force || readBooleanEnv("VERMONT_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isLouisianaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("LOUISIANA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isLouisianaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isLouisianaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("LOUISIANA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isLouisianaCampaignFinanceRawDataRefreshEnabled(force = false): boolean {
  return (
    isLouisianaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("LOUISIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isMichiganCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("MICHIGAN_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isMichiganCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isMichiganCampaignFinanceEnabled() &&
    (force || readBooleanEnv("MICHIGAN_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isMichiganMitnRawDataRefreshEnabled(force = false): boolean {
  return (
    isMichiganCampaignFinanceEnabled() &&
    (force || readBooleanEnv("MICHIGAN_MITN_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isMinnesotaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("MINNESOTA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isMinnesotaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isMinnesotaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("MINNESOTA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isMinnesotaCampaignFinanceRawDataRefreshEnabled(force = false): boolean {
  return (
    isMinnesotaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("MINNESOTA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isPennsylvaniaCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("PENNSYLVANIA_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isPennsylvaniaCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isPennsylvaniaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("PENNSYLVANIA_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}

export function isPennsylvaniaCampaignFinanceRawDataRefreshEnabled(force = false): boolean {
  return (
    isPennsylvaniaCampaignFinanceEnabled() &&
    (force || readBooleanEnv("PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_ENABLED", false))
  );
}

export function isOregonCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("OREGON_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isOregonCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isOregonCampaignFinanceEnabled() &&
    (force || readBooleanEnv("OREGON_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
  );
}
