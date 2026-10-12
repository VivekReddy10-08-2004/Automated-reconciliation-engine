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
        -- Hybrid limits keep an absolute floor for low-priced instruments and
        -- scale for higher-priced equities: WARN > max(0.01, 0.10%) and
        -- BREAK > max(0.05, 0.50%).
        when abs(close_difference) > greatest(
            0.05,
            greatest(abs(yahoo_close), abs(alpaca_close)) * 0.005
        ) then 'BREAK'
        when abs(close_difference) > greatest(
            0.01,
            greatest(abs(yahoo_close), abs(alpaca_close)) * 0.001
        ) then 'WARN'
        else 'MATCH'
    end as status
from differences
