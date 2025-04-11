select
a.order_sk,
b.customer_sk,
c.products_sk,
a.order_date,
a.quantity,
a.total_amount
from project_analytics.stage.orders a
left join (
    select * from 
    project_analytics.processed.dim_customer
    where current_timestamp between effective_from and effective_to
 ) b
on a.customer_id = b.customer_id
left join (
    select * from 
    project_analytics.processed.dim_product
    where current_timestamp between effective_from and effective_to
 ) c
on a.product_id = c.product_id