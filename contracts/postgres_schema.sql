-- stream-aml case store (database: aml). Applied at compose start and in the EKS init Job.
-- Idempotency: alerts keyed by tx_id; one case per (bank, subject account, replay day);
-- case_jobs is the transactional outbox for draft generation (claimed with FOR UPDATE SKIP LOCKED).

CREATE TABLE IF NOT EXISTS cases (
    case_id          BIGSERIAL PRIMARY KEY,
    bank_id          TEXT        NOT NULL,
    subject_key      TEXT        NOT NULL,              -- sender account of the alerted transaction(s)
    case_day         DATE        NOT NULL,              -- UTC day of event time (replay day)
    status           TEXT        NOT NULL DEFAULT 'open'
                     CHECK (status IN ('open', 'drafting', 'ready', 'needs_review', 'decided')),
    max_score        DOUBLE PRECISION NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bank_id, subject_key, case_day)
);

CREATE TABLE IF NOT EXISTS alerts (
    tx_id                 BIGINT PRIMARY KEY,
    case_id               BIGINT REFERENCES cases(case_id),
    bank_id               TEXT        NOT NULL,
    sender_key            TEXT        NOT NULL,
    receiver_key          TEXT        NOT NULL,
    event_ts_ms           BIGINT      NOT NULL,
    amount_paid_usd_cents BIGINT      NOT NULL,
    score                 DOUBLE PRECISION NOT NULL,
    threshold             DOUBLE PRECISION NOT NULL,
    threshold_version     TEXT        NOT NULL,
    model_name            TEXT        NOT NULL,
    model_version         TEXT        NOT NULL,
    spec_version          INT         NOT NULL,
    feature_values        JSONB       NOT NULL,         -- {feature_name: value|null} exactly as scored
    top_contribs          JSONB       NOT NULL,         -- [{feature, value, contribution}]
    replay_emit_ts_ms     BIGINT,
    connector_ts_ms       BIGINT      NOT NULL,
    features_ts_ms        BIGINT      NOT NULL,
    scored_ts_ms          BIGINT      NOT NULL,
    alert_ts_ms           BIGINT      NOT NULL,
    latency_ms            BIGINT,                       -- alert_ts_ms - replay_emit_ts_ms
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS alerts_case_idx ON alerts (case_id);
CREATE INDEX IF NOT EXISTS alerts_bank_ts_idx ON alerts (bank_id, event_ts_ms);

CREATE TABLE IF NOT EXISTS case_jobs (
    job_id        BIGSERIAL PRIMARY KEY,
    case_id       BIGINT      NOT NULL REFERENCES cases(case_id),
    kind          TEXT        NOT NULL DEFAULT 'draft',
    status        TEXT        NOT NULL DEFAULT 'queued'
                  CHECK (status IN ('queued', 'running', 'done', 'failed', 'skipped')),
    attempts      INT         NOT NULL DEFAULT 0,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by     TEXT,
    locked_at     TIMESTAMPTZ,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (case_id, kind)
);
CREATE INDEX IF NOT EXISTS case_jobs_queue_idx ON case_jobs (status, available_at, job_id);

CREATE TABLE IF NOT EXISTS case_evidence (
    case_id      BIGINT PRIMARY KEY REFERENCES cases(case_id),
    as_of_tx_id  BIGINT      NOT NULL,
    as_of_ts_ms  BIGINT      NOT NULL,
    evidence     JSONB       NOT NULL,                  -- {nodes:[...], edges:[...], truncated, total_edges}
    built_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS case_drafts (
    draft_id            BIGSERIAL PRIMARY KEY,
    case_id             BIGINT      NOT NULL REFERENCES cases(case_id),
    attempt             INT         NOT NULL,           -- 1 = first call, 2 = semantic retry
    model               TEXT        NOT NULL,
    prompt_version      TEXT        NOT NULL,
    draft               JSONB,                          -- agent_output.schema.json instance
    validator_status    TEXT        NOT NULL CHECK (validator_status IN ('pass', 'fail', 'error')),
    validator_errors    JSONB       NOT NULL DEFAULT '[]'::jsonb,
    input_tokens        INT,
    output_tokens       INT,
    cache_read_tokens   INT,
    latency_ms          INT,
    cost_usd            NUMERIC(10, 6),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS case_drafts_case_idx ON case_drafts (case_id, draft_id DESC);

CREATE TABLE IF NOT EXISTS decisions (
    decision_id          BIGSERIAL PRIMARY KEY,
    case_id              BIGINT      NOT NULL UNIQUE REFERENCES cases(case_id),
    reviewer             TEXT        NOT NULL,
    decision             TEXT        NOT NULL CHECK (decision IN ('escalate', 'dismiss')),
    reason_code          TEXT        NOT NULL,
    draft_id             BIGINT REFERENCES case_drafts(draft_id),
    edited_narrative     TEXT,
    edit_distance_ratio  DOUBLE PRECISION,              -- normalized Levenshtein vs draft text; NULL if no draft
    ui_mode              TEXT        NOT NULL DEFAULT 'assisted' CHECK (ui_mode IN ('assisted', 'baseline')),
    view_started_at      TIMESTAMPTZ,
    decided_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id   BIGSERIAL PRIMARY KEY,
    case_id    BIGINT REFERENCES cases(case_id),
    actor      TEXT        NOT NULL,                    -- 'system:alerts', 'system:agent', 'reviewer:<name>'
    action     TEXT        NOT NULL,                    -- alert_attached, draft_created, decision_made, ...
    payload    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_case_idx ON audit_log (case_id, at);

CREATE TABLE IF NOT EXISTS study_events (
    event_id   BIGSERIAL PRIMARY KEY,
    case_id    BIGINT      NOT NULL REFERENCES cases(case_id),
    reviewer   TEXT        NOT NULL,
    ui_mode    TEXT        NOT NULL CHECK (ui_mode IN ('assisted', 'baseline')),
    event      TEXT        NOT NULL CHECK (event IN ('view_start', 'decision')),
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =====================================================================================
-- v2 (Phase 3 / M05 + Phase 4 / M07): tenant isolation, roles, maker-checker, HITL loops
-- =====================================================================================
-- Two database roles:
--   aml_service  internal services (alert service, agent worker, exporters). BYPASSRLS.
--   aml_api      the FastAPI backend. Subject to row-level security.
-- Both are created NOLOGIN here. The init step (compose / EKS init Job) adds LOGIN and a
-- password from the environment, so no secret ever lives in this file.
--
-- Per request, inside its transaction, the API runs (bound parameters, never string-built SQL;
-- the third argument `true` makes each setting last only until the transaction ends):
--   SELECT set_config('app.bank_id', $1, true),   -- bank of the signed-in user
--          set_config('app.role',    $2, true),   -- 'analyst' | 'supervisor'
--          set_config('app.user_id', $3, true);   -- user name
-- (`app.user` would not work: USER is a reserved word in SET syntax.)
-- current_setting(..., true) returns NULL or '' when a setting is missing, so a request
-- that forgot to set app.bank_id sees zero rows (fails closed).

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'aml_service') THEN
        CREATE ROLE aml_service NOLOGIN BYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'aml_api') THEN
        CREATE ROLE aml_api NOLOGIN NOBYPASSRLS;
    END IF;
END
$$;

-- Loop 2: structured feedback on LLM drafts (feeds the agent eval set and the prompt gate)
CREATE TABLE IF NOT EXISTS draft_feedback (
    feedback_id          BIGSERIAL PRIMARY KEY,
    case_id              BIGINT      NOT NULL REFERENCES cases(case_id),
    draft_id             BIGINT      NOT NULL REFERENCES case_drafts(draft_id),
    reviewer             TEXT        NOT NULL,
    verdict              TEXT        NOT NULL CHECK (verdict IN ('accepted', 'edited', 'rejected')),
    reason_codes         TEXT[]      NOT NULL DEFAULT '{}',   -- wrong_pattern, ungrounded_claim,
                                                              -- missing_evidence, wrong_amount, unclear, other
    edited_narrative     TEXT,
    edit_distance_ratio  DOUBLE PRECISION,
    in_eval_set          BOOLEAN     NOT NULL DEFAULT false,  -- set by eval/build_eval_set.py
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (draft_id, reviewer)
);

-- Loop 3: QA sample of transactions just below the threshold, plus every needs_review draft
CREATE TABLE IF NOT EXISTS qa_samples (
    sample_id      BIGSERIAL PRIMARY KEY,
    bank_id        TEXT        NOT NULL,
    tx_id          BIGINT      NOT NULL,
    source         TEXT        NOT NULL CHECK (source IN ('below_threshold', 'needs_review')),
    score          DOUBLE PRECISION NOT NULL,
    threshold      DOUBLE PRECISION NOT NULL,
    sampling_rate  DOUBLE PRECISION NOT NULL,                -- needed to turn QA results into a miss-rate estimate
    sampled_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewer       TEXT,
    verdict        TEXT CHECK (verdict IN ('suspicious', 'not_suspicious')),
    reviewed_at    TIMESTAMPTZ,
    UNIQUE (tx_id, source)
);
CREATE INDEX IF NOT EXISTS qa_samples_queue_idx ON qa_samples (bank_id, reviewed_at);

-- Loop 4 and prompt/model changes: maker-checker approvals
CREATE TABLE IF NOT EXISTS approvals (
    approval_id    BIGSERIAL PRIMARY KEY,
    bank_id        TEXT        NOT NULL,
    kind           TEXT        NOT NULL CHECK (kind IN ('threshold_change', 'prompt_change',
                                                        'model_override', 'connector_mapping')),
    payload        JSONB       NOT NULL,                     -- e.g. {"threshold": 0.71, "expected_alerts_per_day": 140,
                                                             --       "expected_recall": 0.62, "source": "operating_point_v3"}
    status         TEXT        NOT NULL DEFAULT 'proposed'
                   CHECK (status IN ('proposed', 'approved', 'rejected', 'applied')),
    proposed_by    TEXT        NOT NULL,
    proposed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_by     TEXT,
    decided_at     TIMESTAMPTZ,
    decision_note  TEXT,
    CONSTRAINT approvals_maker_checker
        CHECK (decided_by IS NULL OR decided_by <> proposed_by)  -- nobody approves their own change
);
CREATE INDEX IF NOT EXISTS approvals_bank_status_idx ON approvals (bank_id, status);

-- Thresholds the alert service serves. A new row appears only by applying an approved change.
CREATE TABLE IF NOT EXISTS bank_thresholds (
    bank_id            TEXT        NOT NULL,
    threshold_version  TEXT        NOT NULL,
    threshold          DOUBLE PRECISION NOT NULL CHECK (threshold > 0 AND threshold < 1),
    approval_id        BIGINT REFERENCES approvals(approval_id), -- NULL only for the M02 initial values
    active_from        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bank_id, threshold_version)
);

-- Row-level security ---------------------------------------------------------------
ALTER TABLE cases           ENABLE ROW LEVEL SECURITY;  ALTER TABLE cases           FORCE ROW LEVEL SECURITY;
ALTER TABLE alerts          ENABLE ROW LEVEL SECURITY;  ALTER TABLE alerts          FORCE ROW LEVEL SECURITY;
ALTER TABLE case_evidence   ENABLE ROW LEVEL SECURITY;  ALTER TABLE case_evidence   FORCE ROW LEVEL SECURITY;
ALTER TABLE case_drafts     ENABLE ROW LEVEL SECURITY;  ALTER TABLE case_drafts     FORCE ROW LEVEL SECURITY;
ALTER TABLE decisions       ENABLE ROW LEVEL SECURITY;  ALTER TABLE decisions       FORCE ROW LEVEL SECURITY;
ALTER TABLE audit_log       ENABLE ROW LEVEL SECURITY;  ALTER TABLE audit_log       FORCE ROW LEVEL SECURITY;
ALTER TABLE study_events    ENABLE ROW LEVEL SECURITY;  ALTER TABLE study_events    FORCE ROW LEVEL SECURITY;
ALTER TABLE draft_feedback  ENABLE ROW LEVEL SECURITY;  ALTER TABLE draft_feedback  FORCE ROW LEVEL SECURITY;
ALTER TABLE qa_samples      ENABLE ROW LEVEL SECURITY;  ALTER TABLE qa_samples      FORCE ROW LEVEL SECURITY;
ALTER TABLE approvals       ENABLE ROW LEVEL SECURITY;  ALTER TABLE approvals       FORCE ROW LEVEL SECURITY;
ALTER TABLE bank_thresholds ENABLE ROW LEVEL SECURITY;  ALTER TABLE bank_thresholds FORCE ROW LEVEL SECURITY;
-- case_jobs has no policy and no grant to aml_api: only services touch the outbox.

-- Tables with their own bank_id
DROP POLICY IF EXISTS tenant_cases ON cases;
CREATE POLICY tenant_cases ON cases TO aml_api
    USING (bank_id = current_setting('app.bank_id', true))
    WITH CHECK (bank_id = current_setting('app.bank_id', true));

DROP POLICY IF EXISTS tenant_alerts ON alerts;
CREATE POLICY tenant_alerts ON alerts TO aml_api
    USING (bank_id = current_setting('app.bank_id', true));

DROP POLICY IF EXISTS tenant_qa ON qa_samples;
CREATE POLICY tenant_qa ON qa_samples TO aml_api
    USING (bank_id = current_setting('app.bank_id', true))
    WITH CHECK (bank_id = current_setting('app.bank_id', true));

DROP POLICY IF EXISTS tenant_thresholds ON bank_thresholds;
CREATE POLICY tenant_thresholds ON bank_thresholds FOR SELECT TO aml_api
    USING (bank_id = current_setting('app.bank_id', true));

-- Tables keyed by case_id inherit the case's bank
DROP POLICY IF EXISTS tenant_evidence ON case_evidence;
CREATE POLICY tenant_evidence ON case_evidence TO aml_api
    USING (EXISTS (SELECT 1 FROM cases c WHERE c.case_id = case_evidence.case_id
                   AND c.bank_id = current_setting('app.bank_id', true)));

DROP POLICY IF EXISTS tenant_drafts ON case_drafts;
CREATE POLICY tenant_drafts ON case_drafts TO aml_api
    USING (EXISTS (SELECT 1 FROM cases c WHERE c.case_id = case_drafts.case_id
                   AND c.bank_id = current_setting('app.bank_id', true)));

DROP POLICY IF EXISTS tenant_decisions ON decisions;
CREATE POLICY tenant_decisions ON decisions TO aml_api
    USING (EXISTS (SELECT 1 FROM cases c WHERE c.case_id = decisions.case_id
                   AND c.bank_id = current_setting('app.bank_id', true)))
    WITH CHECK (EXISTS (SELECT 1 FROM cases c WHERE c.case_id = decisions.case_id
                        AND c.bank_id = current_setting('app.bank_id', true))
                AND reviewer = current_setting('app.user_id', true));

DROP POLICY IF EXISTS tenant_audit ON audit_log;
CREATE POLICY tenant_audit ON audit_log TO aml_api
    USING (EXISTS (SELECT 1 FROM cases c WHERE c.case_id = audit_log.case_id
                   AND c.bank_id = current_setting('app.bank_id', true)));

DROP POLICY IF EXISTS tenant_study ON study_events;
CREATE POLICY tenant_study ON study_events TO aml_api
    USING (EXISTS (SELECT 1 FROM cases c WHERE c.case_id = study_events.case_id
                   AND c.bank_id = current_setting('app.bank_id', true)));

DROP POLICY IF EXISTS tenant_feedback ON draft_feedback;
CREATE POLICY tenant_feedback ON draft_feedback TO aml_api
    USING (EXISTS (SELECT 1 FROM cases c WHERE c.case_id = draft_feedback.case_id
                   AND c.bank_id = current_setting('app.bank_id', true)))
    WITH CHECK (EXISTS (SELECT 1 FROM cases c WHERE c.case_id = draft_feedback.case_id
                        AND c.bank_id = current_setting('app.bank_id', true))
                AND reviewer = current_setting('app.user_id', true));

-- Approvals: anyone in the bank can see and propose; only a supervisor can decide.
-- No FOR ALL policy here, because permissive policies are OR-ed together.
DROP POLICY IF EXISTS approvals_read ON approvals;
CREATE POLICY approvals_read ON approvals FOR SELECT TO aml_api
    USING (bank_id = current_setting('app.bank_id', true));

DROP POLICY IF EXISTS approvals_propose ON approvals;
CREATE POLICY approvals_propose ON approvals FOR INSERT TO aml_api
    WITH CHECK (bank_id = current_setting('app.bank_id', true)
                AND status = 'proposed'
                AND proposed_by = current_setting('app.user_id', true));

DROP POLICY IF EXISTS approvals_decide ON approvals;
CREATE POLICY approvals_decide ON approvals FOR UPDATE TO aml_api
    USING (bank_id = current_setting('app.bank_id', true)
           AND status = 'proposed'
           AND current_setting('app.role', true) = 'supervisor')
    WITH CHECK (bank_id = current_setting('app.bank_id', true)
                AND status IN ('approved', 'rejected')
                AND decided_by = current_setting('app.user_id', true));

-- Grants --------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO aml_api, aml_service;
GRANT SELECT ON cases, alerts, case_evidence, case_drafts, bank_thresholds TO aml_api;
GRANT UPDATE (status, updated_at) ON cases TO aml_api;
GRANT SELECT, INSERT ON decisions, audit_log, study_events, draft_feedback TO aml_api;
GRANT SELECT, UPDATE (reviewer, verdict, reviewed_at) ON qa_samples TO aml_api;
GRANT SELECT, INSERT ON approvals TO aml_api;
GRANT UPDATE (status, decided_by, decided_at, decision_note) ON approvals TO aml_api;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO aml_api;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO aml_service;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO aml_service;
