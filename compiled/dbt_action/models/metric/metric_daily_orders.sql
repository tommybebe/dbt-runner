
select * 
from -- Need this here, since the actual ref is nested within loops/conditions:
    -- depends on: `minwoo-lee`.`jaffle_shop`.`dbt_metrics_default_calendar`
    (with source_query as (

    select
        /* Always trunc to the day, then use dimensions on calendar table to achieve the _actual_ desired aggregates. */
        /* Need to cast as a date otherwise we get values like 2021-01-01 and 2021-01-01T00:00:00+00:00 that don't join :( */
        cast(
    timestamp_trunc(
        cast(cast(order_date as date) as timestamp),
        day
    )

 as date) as date_day,
        
        status,
            order_id as property_to_aggregate

    from `minwoo-lee`.`jaffle_shop`.`orders`
    where 1=1
    
    
),

 spine__time as (
     select 
        /* this could be the same as date_day if grain is day. That's OK! 
        They're used for different things: date_day for joining to the spine, period for aggregating.*/
        date_day as period, 
        
            date_month,
        
            date_year,
        
        
        date_day
     from `minwoo-lee`.`jaffle_shop`.`dbt_metrics_default_calendar`
 ),
          
        spine__values__status as (

            select distinct status
            from source_query

        ),  
    

spine as (

    select *
    from spine__time
            cross join spine__values__status

),

joined as (
    select 
        spine.period,
        
        spine.date_month,
        
        spine.date_year,
        
        
        spine.status,
        

        -- has to be aggregated in this CTE to allow dimensions coming from the calendar table
    count(source_query.property_to_aggregate)
 as daily_orders,
        logical_or(source_query.date_day is not null) as has_data

    from spine
    left outer join source_query on source_query.date_day = spine.date_day
    
            and source_query.status = spine.status
    
    group by 1, 2, 3, 4

),

bounded as (
    select 
        *,
         min(case when has_data then period end) over ()  as lower_bound,
         max(case when has_data then period end) over ()  as upper_bound
    from joined 
),

secondary_calculations as (

    select *
        
        , 
    cast(coalesce(daily_orders, 0) / nullif(
        lag(daily_orders, 1) over (
            partition by status 
            order by period
        )
    , 0) as 
    float64
)


as ratio_to_1_day_ago

        , 
    coalesce(daily_orders, 0) - coalesce(
        lag(daily_orders, 1) over (
            partition by status 
            order by period
        )
    , 0)


as difference_to_1_day_ago

        , 
    avg(daily_orders)
over (
            partition by date_month
            , status
            order by period
            rows between unbounded preceding and current row
        )

as average_for_month

        , 
    sum(daily_orders)
over (
            partition by date_year
            , status
            order by period
            rows between unbounded preceding and current row
        )

as sum_for_year

        , 
        
    avg(daily_orders)

        over (
            partition by status 
            order by period
            rows between 3 preceding and current row
        )
    

as rolling_average_4_day

        , 
        
    min(daily_orders)

        over (
            partition by status 
            order by period
            rows between 3 preceding and current row
        )
    

as rolling_min_4_day

        

    from bounded
    
),

final as (
    select
        period
        
        , status
        
        , coalesce(daily_orders, 0) as daily_orders
        
        , ratio_to_1_day_ago
        
        , difference_to_1_day_ago
        
        , average_for_month
        
        , sum_for_year
        
        , rolling_average_4_day
        
        , rolling_min_4_day
        

    from secondary_calculations
    where period >= lower_bound
    and period <= upper_bound
    order by 1, 2
)

select * from final

) metric_subq