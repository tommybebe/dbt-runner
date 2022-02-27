{{ config(materialized='table') }}
select * 
from {{ metrics.metric(
    metric_name='daily_orders',
    grain='day',
    dimensions=['status'],
    secondary_calculations=[
        metrics.period_over_period(comparison_strategy="ratio", interval=1),
        metrics.period_over_period(comparison_strategy="difference", interval=1),

        metrics.period_to_date(aggregate="average", period="month"),
        metrics.period_to_date(aggregate="sum", period="year"),

        metrics.rolling(aggregate="average", interval=4),
        metrics.rolling(aggregate="min", interval=4)
    ]
) }}