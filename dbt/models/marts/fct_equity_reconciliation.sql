with differences as (
    select
        run_id,
        symbol,
        trade_date,
        source_a,
        source_b,
        source_a_close,
        source_b_close,
        source_a_close - source_b_close as close_difference,
        source_a_volume,
        source_b_volume,
        source_a_volume - source_b_volume as volume_difference
    from {{ ref('int_equity_pairs') }}
)

select
    run_id,
    symbol,
    trade_date,
    source_a,
    source_b,
    source_a_close,
    source_b_close,
    close_difference,
    source_a_volume,
    source_b_volume,
    volume_difference,
    case
        when source_a_close is null or source_b_close is null then 'BREAK'
        -- Hybrid limits keep an absolute floor for low-priced instruments and
        -- scale for higher-priced equities: WARN > max(0.01, 0.10%) and
        -- BREAK > max(0.05, 0.50%).
        when abs(close_difference) > greatest(
            0.05,
            greatest(abs(source_a_close), abs(source_b_close)) * 0.005
        ) then 'BREAK'
        when abs(close_difference) > greatest(
            0.01,
            greatest(abs(source_a_close), abs(source_b_close)) * 0.001
        ) then 'WARN'
        else 'MATCH'
    end as status,
    case
        when source_a_close is null then 'MISSING_' || source_a || '_OBSERVATION'
        when source_b_close is null then 'MISSING_' || source_b || '_OBSERVATION'
        when abs(close_difference) > greatest(
            0.05,
            greatest(abs(source_a_close), abs(source_b_close)) * 0.005
        ) then 'PRICE_DIFFERENCE'
        else null
    end as break_reason
from differences
