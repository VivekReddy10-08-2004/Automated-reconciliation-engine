with differences as (
    select
        run_id,
        symbol,
        trade_date,
        yahoo_close,
        alpaca_close,
        yahoo_close - alpaca_close as close_difference,
        yahoo_volume,
        alpaca_volume,
        yahoo_volume - alpaca_volume as volume_difference
    from {{ ref('int_equity_pairs') }}
)

select
    run_id,
    symbol,
    trade_date,
    yahoo_close,
    alpaca_close,
    close_difference,
    yahoo_volume,
    alpaca_volume,
    volume_difference,
    case
        when close_difference is null then 'BREAK'
        -- Control thresholds are absolute close differences in price units:
        -- WARN above 0.01 and BREAK above 0.05.
        when abs(close_difference) > 0.05 then 'BREAK'
        when abs(close_difference) > 0.01 then 'WARN'
        else 'MATCH'
    end as status
from differences
