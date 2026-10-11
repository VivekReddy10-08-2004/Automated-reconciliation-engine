with yahoo as (
    select *
    from {{ ref('stg_equity_bars') }}
    where source = 'YAHOO'
),

alpaca as (
    select *
    from {{ ref('stg_equity_bars') }}
    where source = 'ALPACA'
)

select
    coalesce(yahoo.run_id, alpaca.run_id) as run_id,
    coalesce(yahoo.symbol, alpaca.symbol) as symbol,
    coalesce(yahoo.trade_date, alpaca.trade_date) as trade_date,
    yahoo.close as yahoo_close,
    alpaca.close as alpaca_close,
    yahoo.volume as yahoo_volume,
    alpaca.volume as alpaca_volume
from yahoo
full outer join alpaca
    on yahoo.run_id = alpaca.run_id
    and yahoo.symbol = alpaca.symbol
    and yahoo.trade_date = alpaca.trade_date
