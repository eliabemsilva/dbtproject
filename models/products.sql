select
p.product_id, p.product_name, p.unit_price, c.category_name, s.company_name suppliers
from {{source('sources', 'products')}} p
left join {{source('sources', 'suppliers')}} s 
on (p.supplier_id = s.supplier_id)
left join {{source('sources', 'categories')}} c
on (p.category_id = c.category_id)