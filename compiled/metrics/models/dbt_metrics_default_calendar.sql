

--TODO: Don't want to depend on utils long term.
with days as (
    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
     + 
    
    p12.generated_number * power(2, 12)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
     cross join 
    
    p as p12
    
    

    )

    select *
    from unioned
    where generated_number <= 7305
    order by generated_number



),

all_periods as (

    select (
        

        datetime_add(
            cast( cast('2010-01-01' as date) as datetime),
        interval row_number() over (order by 1) - 1 day
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= cast('2030-01-01' as date)

)

select * from filtered


),

final as (
    select 
        cast(date_day as date) as date_day,
        cast(
    timestamp_trunc(
        cast(date_day as timestamp),
        week
    )

 as date) as date_week,
        cast(
    timestamp_trunc(
        cast(date_day as timestamp),
        month
    )

 as date) as date_month,
        cast(
    timestamp_trunc(
        cast(date_day as timestamp),
        quarter
    )

 as date) as date_quarter,
        cast(
    timestamp_trunc(
        cast(date_day as timestamp),
        year
    )

 as date) as date_year
    from days
)

select * from final