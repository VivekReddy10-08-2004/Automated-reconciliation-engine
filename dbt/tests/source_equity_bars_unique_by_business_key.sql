-- Staging deliberately selects one deterministic row, but duplicated provider
-- observations are still a source-data defect and must make `dbt build` fail.
select
    run_id,
    upper(trim(source)) as source,
    trim(symbol) as symbol,
    cast("timestamp" as date) as trade_date,
    count(*) as row_count
from {{ source('reconciliation', 'equity_bars_staging') }}
group by 1, 2, 3, 4
having count(*) > 1
