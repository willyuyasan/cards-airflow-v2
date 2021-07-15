insert into cccomus.summarized_transactions (
transaction_date,
product_id,
product_name,
program_id,
program_name,
product_type_id,
product_type_name,
category_fid,
category_name,
website_id,
affiliate_id,
website_url,
user_variable,
click_count,
application_count_cd,
application_count_pd,
sale_count_cd,
sale_count_pd,
sale_amount_cd,
sale_amount_pd,
adjustment_amount_cd,
adjustment_amount_pd,
created_date,
hashed_key
)
select sts.transaction_date,
sts.product_id,
sts.product_name,
sts.program_id,
sts.program_name,
sts.product_type_id,
sts.product_type_name,
sts.category_fid,
sts.category_name,
sts.website_id,
sts.affiliate_id,
sts.website_url,
sts.user_variable,
sts.click_count,
sts.application_count_cd,
sts.application_count_pd,
sts.sale_count_cd,
sts.sale_count_pd,
sts.sale_amount_cd,
sts.sale_amount_pd,
sts.adjustment_amount_cd,
sts.adjustment_amount_pd,
now(),
sts.hashed_key
from cccomus.summarized_transactions_stage sts
left join cccomus.summarized_transactions st using (hashed_key)
where sts.transaction_date between {{params["START_DATE"]}} AND {{params["END_DATE"]}}
and (st.transaction_date is null
or st.product_name != sts.product_name
or st.program_name != sts.program_name
or st.product_type_name!= sts.product_type_name
or st.category_name != sts.category_name
or st.website_url != sts.website_url
or st.click_count != sts.click_count
or st.application_count_cd != sts.application_count_cd
or st.application_count_pd != sts.application_count_pd
or st.sale_count_cd != sts.sale_count_cd
or st.sale_count_pd != sts.sale_count_pd
or st.sale_amount_cd != sts.sale_amount_cd
or st.sale_amount_pd != sts.sale_amount_pd
or st.adjustment_amount_cd != sts.adjustment_amount_cd
or st.adjustment_amount_pd != sts.adjustment_amount_pd
)on duplicate key update
product_name = sts.product_name,
program_name = sts.program_name,
product_type_name = sts.product_type_name,
category_name = sts.category_name,
website_url = sts.website_url,
click_count = sts.click_count,
application_count_cd = sts.application_count_cd,
application_count_pd = sts.application_count_pd,
sale_count_cd = sts.sale_count_cd,
sale_count_pd = sts.sale_count_pd,
sale_amount_cd = sts.sale_amount_cd,
sale_amount_pd = sts.sale_amount_pd,
adjustment_amount_cd = sts.adjustment_amount_cd,
adjustment_amount_pd = sts.adjustment_amount_pd,
updated_date = now();

delete from cccomus.summarized_transactions
where transaction_date between {{params["START_DATE"]}} AND {{params["END_DATE"]}}
and hashed_key not in (
  select hashed_key
  from cccomus.summarized_transactions_stage
) and exists (
  select 1
  from cccomus.summarized_transactions_stage
  where transaction_date between {{params["START_DATE"]}} AND {{params["END_DATE"]}}
);

REPLACE INTO cccomus.summarized_transaction_dates (transaction_type,
summarized_date)
SELECT REPLACE(transaction_type,'_stage',''), summarized_date
FROM cccomus.summarized_transaction_dates
WHERE transaction_type LIKE '%_stage';
