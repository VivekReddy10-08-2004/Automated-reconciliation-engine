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
    yahoo.run_id,
    yahoo.symbol,
    yahoo.trade_date,
    yahoo.close as yahoo_close,
    alpaca.close as alpaca_close,
    yahoo.volume as yahoo_volume,
    alpaca.volume as alpaca_volume
from yahoo
inner join alpaca
    on yahoo.run_id = alpaca.run_id
    and yahoo.symbol = alpaca.symbol
    and yahoo.trade_date = alpaca.trade_date
