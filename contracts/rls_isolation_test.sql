-- Tenant-isolation and maker-checker test for contracts/postgres_schema.sql (v2).
-- Run against an EMPTY throwaway database after applying the schema:
--   psql -v ON_ERROR_STOP=1 -d aml_test -f contracts/postgres_schema.sql
--   psql -v ON_ERROR_STOP=1 -d aml_test -f contracts/rls_isolation_test.sql
-- Exits non-zero on the first failed expectation. M05's isolation test wraps this file.

INSERT INTO cases (bank_id, subject_key, case_day, max_score)
VALUES ('A', 'a1', '2022-09-01', 0.90), ('B', 'b1', '2022-09-01', 0.95);
INSERT INTO case_drafts (case_id, attempt, model, prompt_version, validator_status)
SELECT case_id, 1, 'claude-haiku-5-5', 'v1', 'pass' FROM cases;

-- Remember the case ids as session settings, so the checks below don't need to switch roles
SELECT set_config('test.a_case', (SELECT case_id FROM cases WHERE bank_id = 'A')::text, false),
       set_config('test.b_case', (SELECT case_id FROM cases WHERE bank_id = 'B')::text, false);

SET ROLE aml_api;

DO $$
DECLARE n int; a_case bigint; b_case bigint;
BEGIN
    -- 1. No tenant set: fail closed
    SELECT count(*) INTO n FROM cases;
    IF n <> 0 THEN RAISE EXCEPTION 'T1 no app.bank_id should see 0 cases, saw %', n; END IF;

    -- 2. Analyst at bank A
    PERFORM set_config('app.bank_id', 'A', true), set_config('app.role', 'analyst', true),
            set_config('app.user_id', 'ann', true);
    SELECT count(*) INTO n FROM cases;
    IF n <> 1 THEN RAISE EXCEPTION 'T2 bank A should see 1 case, saw %', n; END IF;
    SELECT count(*) INTO n FROM case_drafts d JOIN cases c USING (case_id) WHERE c.bank_id = 'B';
    IF n <> 0 THEN RAISE EXCEPTION 'T3 bank A sees % bank B drafts', n; END IF;
    a_case := current_setting('test.a_case')::bigint;
    b_case := current_setting('test.b_case')::bigint;

    BEGIN
        INSERT INTO decisions (case_id, reviewer, decision, reason_code) VALUES (b_case, 'ann', 'dismiss', 't');
        RAISE EXCEPTION 'T4 bank A analyst wrote a decision on a bank B case';
    EXCEPTION WHEN insufficient_privilege THEN NULL;  -- RLS violation is SQLSTATE 42501
    END;
    BEGIN
        INSERT INTO decisions (case_id, reviewer, decision, reason_code) VALUES (a_case, 'mallory', 'dismiss', 't');
        RAISE EXCEPTION 'T5 a decision was recorded under another user''s name';
    EXCEPTION WHEN insufficient_privilege THEN NULL;
    END;
    INSERT INTO decisions (case_id, reviewer, decision, reason_code) VALUES (a_case, 'ann', 'dismiss', 't');

    -- 3. Maker-checker on a threshold change
    INSERT INTO approvals (bank_id, kind, payload, proposed_by)
    VALUES ('A', 'threshold_change', '{"threshold": 0.70}', 'ann');
    PERFORM set_config('app.user_id', 'al', true);       -- a different analyst tries to approve
    UPDATE approvals SET status = 'approved', decided_by = 'al', decided_at = now();
    GET DIAGNOSTICS n = ROW_COUNT;
    IF n <> 0 THEN RAISE EXCEPTION 'T6 an analyst approved a change'; END IF;

    PERFORM set_config('app.role', 'supervisor', true),  -- the proposer, now as supervisor
            set_config('app.user_id', 'ann', true);
    BEGIN
        UPDATE approvals SET status = 'approved', decided_by = 'ann', decided_at = now();
        RAISE EXCEPTION 'T7 the proposer approved their own change';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    PERFORM set_config('app.user_id', 'sam', true);
    UPDATE approvals SET status = 'approved', decided_by = 'sam', decided_at = now();
    GET DIAGNOSTICS n = ROW_COUNT;
    IF n <> 1 THEN RAISE EXCEPTION 'T8 a different supervisor could not approve'; END IF;

    -- 4. Supervisor at bank B sees nothing of bank A
    PERFORM set_config('app.bank_id', 'B', true), set_config('app.user_id', 'bob', true);
    SELECT count(*) INTO n FROM approvals;
    IF n <> 0 THEN RAISE EXCEPTION 'T9 bank B sees % bank A approvals', n; END IF;
    SELECT count(*) INTO n FROM decisions;
    IF n <> 0 THEN RAISE EXCEPTION 'T10 bank B sees % bank A decisions', n; END IF;

    -- 5. The outbox is not reachable from the API role
    BEGIN
        PERFORM 1 FROM case_jobs LIMIT 1;
        RAISE EXCEPTION 'T11 aml_api can read case_jobs';
    EXCEPTION WHEN insufficient_privilege THEN NULL;
    END;

    RAISE NOTICE 'rls_isolation_test: all 11 checks passed';
END
$$;

RESET ROLE;
