select
id,
coalesce (code, '') as code,
coalesce (description, '') as description,
coalesce (cast(create_dt as varchar), '') as create_dt,
coalesce (create_by, '') as create_by
from transactions.transaction_event_types;
