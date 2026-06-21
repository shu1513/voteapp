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
