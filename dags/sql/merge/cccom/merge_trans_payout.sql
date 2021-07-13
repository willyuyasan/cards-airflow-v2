begin transaction;

truncate table cccom_dw.map_trans_payout;

insert into cccom_dw.map_trans_payout (trans_id, payout_id)
select s.trans_id, s.payout_id
from cccom_dw.stg_partner_payout_transaction_map s
where not exists (
  select 1
  from cccom_dw.map_trans_payout f
  where f.trans_id = s.trans_id
  and f.payout_id = s.payout_id 
);

end transaction;
