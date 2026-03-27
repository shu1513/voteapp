type StageName = "producer" | "enricher" | "validator" | "writer";

type StageRunContext = {
  run_id?: string | null;
  provider?: string | null;
  model?: string | null;
  prompt_version?: string | null;
};

type StageRecord = {
  outcome: string;
  ingest_key?: string | null;
  run_id?: string | null;
  provider?: string | null;
  model?: string | null;
  prompt_version?: string | null;
  schema_version?: string | null;
  reason?: string | null;
  duration_ms?: number;
};

type StageSummary = {
  total: number;
  outcomes: Record<string, number>;
  reasons: Record<string, number>;
};

type AlertThresholds = {
  minEvents: number;
  rejectRate: number;
  retryRate: number;
  failRate: number;
  reasonSpikeCount: number;
};

const DEFAULT_ALERT_THRESHOLDS: AlertThresholds = {
  minEvents: 20,
  rejectRate: 0.25,
  retryRate: 0.25,
  failRate: 0.25,
  reasonSpikeCount: 10,
};

function readPositiveNumberEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseFloat(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function readThresholds(): AlertThresholds {
  return {
    minEvents: Math.max(1, Math.floor(readPositiveNumberEnv("OBS_SPIKE_MIN_EVENTS", DEFAULT_ALERT_THRESHOLDS.minEvents))),
    rejectRate: Math.min(1, readPositiveNumberEnv("OBS_REJECT_SPIKE_RATE", DEFAULT_ALERT_THRESHOLDS.rejectRate)),
    retryRate: Math.min(1, readPositiveNumberEnv("OBS_RETRY_SPIKE_RATE", DEFAULT_ALERT_THRESHOLDS.retryRate)),
    failRate: Math.min(1, readPositiveNumberEnv("OBS_FAILED_SPIKE_RATE", DEFAULT_ALERT_THRESHOLDS.failRate)),
    reasonSpikeCount: Math.max(
      1,
      Math.floor(readPositiveNumberEnv("OBS_REASON_SPIKE_COUNT", DEFAULT_ALERT_THRESHOLDS.reasonSpikeCount))
    ),
  };
}

export function bucketReasonForObservability(reason: string): string {
  const trimmed = reason.trim();
  if (!trimmed) {
    return "UNKNOWN";
  }

  const codeMatch = /^\[([A-Z0-9_ -]{2,60})\]/.exec(trimmed);
  if (codeMatch?.[1]) {
    return codeMatch[1];
  }

  return trimmed.length > 160 ? `${trimmed.slice(0, 157)}...` : trimmed;
}

export class StageObserver {
  private readonly stage: StageName;
  private readonly runContext: StageRunContext;
  private readonly startedAtMs: number;
  private readonly outcomeCounts: Map<string, number>;
  private readonly reasonCounts: Map<string, number>;
  private readonly thresholds: AlertThresholds;

  constructor(stage: StageName, runContext: StageRunContext = {}) {
    this.stage = stage;
    this.runContext = runContext;
    this.startedAtMs = Date.now();
    this.outcomeCounts = new Map<string, number>();
    this.reasonCounts = new Map<string, number>();
    this.thresholds = readThresholds();
  }

  record(event: StageRecord): void {
    const outcome = event.outcome.trim().toLowerCase();
    this.outcomeCounts.set(outcome, (this.outcomeCounts.get(outcome) ?? 0) + 1);

    const reason = typeof event.reason === "string" ? event.reason.trim() : "";
    if (reason.length > 0) {
      const bucket = bucketReasonForObservability(reason);
      this.reasonCounts.set(bucket, (this.reasonCounts.get(bucket) ?? 0) + 1);
    }

    console.log(
      JSON.stringify({
        type: "pipeline_event",
        ts: new Date().toISOString(),
        stage: this.stage,
        outcome,
        ingest_key: event.ingest_key ?? null,
        run_id: event.run_id ?? this.runContext.run_id ?? null,
        provider: event.provider ?? this.runContext.provider ?? null,
        model: event.model ?? this.runContext.model ?? null,
        prompt_version: event.prompt_version ?? this.runContext.prompt_version ?? null,
        schema_version: event.schema_version ?? null,
        reason: reason.length > 0 ? reason : null,
        duration_ms: Number.isFinite(event.duration_ms) ? event.duration_ms : null,
      })
    );
  }

  flush(extra: Record<string, unknown> = {}): StageSummary {
    const outcomes = Object.fromEntries(this.outcomeCounts.entries());
    const reasons = Object.fromEntries(this.reasonCounts.entries());
    const total = Array.from(this.outcomeCounts.values()).reduce((sum, value) => sum + value, 0);
    const durationMs = Date.now() - this.startedAtMs;

    const summary: StageSummary = { total, outcomes, reasons };

    console.log(
      JSON.stringify({
        type: "pipeline_summary",
        ts: new Date().toISOString(),
        stage: this.stage,
        total,
        outcomes,
        reasons,
        run_id: this.runContext.run_id ?? null,
        provider: this.runContext.provider ?? null,
        model: this.runContext.model ?? null,
        prompt_version: this.runContext.prompt_version ?? null,
        duration_ms: durationMs,
        ...extra,
      })
    );

    this.emitAlerts(summary);
    return summary;
  }

  private emitAlerts(summary: StageSummary): void {
    if (summary.total < this.thresholds.minEvents) {
      return;
    }

    const rejected = summary.outcomes.rejected ?? 0;
    const retried = summary.outcomes.retry ?? 0;
    const failed = summary.outcomes.failed ?? 0;
    const rejectRate = rejected / summary.total;
    const retryRate = retried / summary.total;
    const failRate = failed / summary.total;

    if (rejectRate >= this.thresholds.rejectRate) {
      console.error(
        JSON.stringify({
          type: "pipeline_alert",
          ts: new Date().toISOString(),
          stage: this.stage,
          alert: "reject_spike",
          total: summary.total,
          rejected,
          reject_rate: rejectRate,
          threshold: this.thresholds.rejectRate,
        })
      );
    }

    if (retryRate >= this.thresholds.retryRate) {
      console.error(
        JSON.stringify({
          type: "pipeline_alert",
          ts: new Date().toISOString(),
          stage: this.stage,
          alert: "retry_spike",
          total: summary.total,
          retried,
          retry_rate: retryRate,
          threshold: this.thresholds.retryRate,
        })
      );
    }

    if (failRate >= this.thresholds.failRate) {
      console.error(
        JSON.stringify({
          type: "pipeline_alert",
          ts: new Date().toISOString(),
          stage: this.stage,
          alert: "failed_spike",
          total: summary.total,
          failed,
          fail_rate: failRate,
          threshold: this.thresholds.failRate,
        })
      );
    }

    for (const [reason, count] of Object.entries(summary.reasons)) {
      if (count >= this.thresholds.reasonSpikeCount) {
        console.error(
          JSON.stringify({
            type: "pipeline_alert",
            ts: new Date().toISOString(),
            stage: this.stage,
            alert: "reason_spike",
            reason,
            count,
            threshold: this.thresholds.reasonSpikeCount,
          })
        );
      }
    }
  }
}

export function createStageObserver(stage: StageName, runContext: StageRunContext = {}): StageObserver {
  return new StageObserver(stage, runContext);
}
