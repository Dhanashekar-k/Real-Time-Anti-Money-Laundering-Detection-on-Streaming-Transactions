# services/

Long-running services, one directory each: `scorer/`, `alerts/`, `agent/` (Claude case-draft worker plus validator), `api/`. `common/` will hold shared code, including `db.py:tenant_tx`.

Tenant isolation (CLAUDE.md rule 11): the API touches Postgres only as `aml_api`, only through `services/common/db.py:tenant_tx`. Added from M5.
