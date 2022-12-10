with calc_employees as (
    select *,
    datepart(year, current_date) - datepart(year, birth_date) age,
    datepart(year, current_date) - datepart(year, hire_date) length_of_service,
    first_name || ' ' || last_name full_name
    from {{source('sources', 'employees')}}
)

select * from calc_employees