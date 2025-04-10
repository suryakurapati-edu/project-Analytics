select
a.order_sk,
b.customer_sk,
a.product_id,
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