with provider_pairs(source_a, source_b) as (
    values
        ('YAHOO', 'ALPACA'),
        ('YAHOO', 'MASSIVE'),
        ('ALPACA', 'MASSIVE')
),

business_keys as (
    select distinct run_id, symbol, trade_date
    from {{ ref('stg_equity_bars') }}
),

expected_comparisons as (
    select keys.*, pairs.source_a, pairs.source_b
    from business_keys as keys
    cross join provider_pairs as pairs
)

select
    expected.run_id,
    expected.symbol,
    expected.trade_date,
    expected.source_a,
    expected.source_b,
    source_a.close as source_a_close,
    source_b.close as source_b_close,
    source_a.volume as source_a_volume,
    source_b.volume as source_b_volume
from expected_comparisons as expected
left join {{ ref('stg_equity_bars') }} as source_a
    on expected.run_id = source_a.run_id
    and expected.symbol = source_a.symbol
    and expected.trade_date = source_a.trade_date
    and expected.source_a = source_a.source
left join {{ ref('stg_equity_bars') }} as source_b
    on expected.run_id = source_b.run_id
    and expected.symbol = source_b.symbol
    and expected.trade_date = source_b.trade_date
    and expected.source_b = source_b.source
