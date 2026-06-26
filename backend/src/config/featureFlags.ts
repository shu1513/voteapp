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

export function isNewMexicoCampaignFinanceEnabled(): boolean {
  return readBooleanEnv("NEW_MEXICO_CAMPAIGN_FINANCE_ENABLED", false);
}

export function isNewMexicoCampaignFinanceSyncEnabled(force = false): boolean {
  return (
    isNewMexicoCampaignFinanceEnabled() &&
    (force || readBooleanEnv("NEW_MEXICO_CAMPAIGN_FINANCE_SYNC_ENABLED", false))
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

export function isTexasTecRawDataRefreshEnabled(force = false): boolean {
  return (
    isTexasCampaignFinanceEnabled() &&
    (force || readBooleanEnv("TEXAS_TEC_RAW_DATA_REFRESH_ENABLED", false))
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
