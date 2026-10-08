-- Warns when PostgreSQL and the Kafka events disagree about any order.
-- Short-lived mismatches are normal while events are still on their way
-- (DB updates instantly; events arrive after upload + job). Re-run later.
{{ config(severity='warn') }}

select *
from {{ ref('rpt_order_reconciliation') }}
where reconciliation_status <> 'ok'
