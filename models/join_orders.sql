with orddetails as (
    select
    odt.order_id, odt.product_id, odt.unit_price, odt.quantity,
    odt.unit_price * odt.quantity total,
    pd.unit_price * odt.quantity - total discount 
    from {{source('sources', 'order_details')}} odt
    left join {{ref('products')}} pd
    on (odt.product_id = pd.product_id)
),
    joinorders as (
    select od.*, ord.order_date, ord.customer_id, ord.employee_id, ord.ship_via
    from orddetails od
    inner join {{source('sources', 'orders')}} ord on (od.order_id = ord.order_id)
)

select * from joinorders