## What's new in V5.2 — CI/CD for dbt
- **dbt CI** (`.github/workflows/dbt-ci.yml`): every PR builds and tests only the changed models
  (`state:modified+ --defer`) in a temporary schema `ci_pr_<N>`; schemas are dropped when the PR closes
- **dbt deploy** (`.github/workflows/dbt-deploy.yml`): every merge to `main` rebuilds production
  (`kafka_store_v5_dbt_*`) and publishes the dbt docs as a build artifact
- `main` is protected: merging requires the `dbt-build` check to pass

## What's new in V5.1 — dbt
- dbt project `dbt_kafka_store/` builds staging views and mart tables from the Bronze tables
- Tests: unique / not_null / accepted_values / relationships + custom SQL tests
- Source freshness on Bronze (`_loaded_at`)
- Incremental model `fct_order_events` (merge on `event_id`)
- Run with `python run_dbt.py build`; docs with `python run_dbt.py docs serve`