## What's new in V5.1 — dbt
- dbt project `dbt_kafka_store/` builds staging views and mart tables from the Bronze tables
- Tests: unique / not_null / accepted_values / relationships + custom SQL tests
- Source freshness on Bronze (`_loaded_at`)
- Incremental model `fct_order_events` (merge on `event_id`)
- Run with `python run_dbt.py build`; docs with `python run_dbt.py docs serve`