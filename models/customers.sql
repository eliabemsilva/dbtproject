with markup as (
    select * ,
    first_value(customer_id)
    over(partition by company_name, contact_name
    order by company_name
    rows between unbounded preceding and unbounded following) as duplicate_results
    from {{source('sources', 'customers')}}
),
    removed as (
    select distinct duplicate_results 
    from markup
),
    final as (
    select * 
    from {{source('sources', 'customers')}}
    where customer_id in (select duplicate_results from removed)
)
select * from final