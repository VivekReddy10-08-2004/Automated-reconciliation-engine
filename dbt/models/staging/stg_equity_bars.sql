with typed_source as (
    select
        cast(run_id as varchar) as run_id,
        upper(trim(cast(source as varchar))) as source,
        trim(cast(symbol as varchar)) as symbol,
        cast("timestamp" as date) as trade_date,
        cast(open as double) as open,
        cast(high as double) as high,
        cast(low as double) as low,
        cast(close as double) as close,
        cast(volume as bigint) as volume
    from {{ source('reconciliation', 'equity_bars_staging') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by run_id, source, symbol, trade_date
            order by
                close desc nulls last,
                volume desc nulls last,
                open desc nulls last,
                high desc nulls last,
                low desc nulls last
        ) as duplicate_rank
    from typed_source
)

select
    run_id || '|' || source || '|' || symbol || '|' || cast(trade_date as varchar)
        as bar_key,
    run_id,
    source,
    symbol,
    trade_date,
    open,
    high,
    low,
    close,
    volume
from ranked
where duplicate_rank = 1
