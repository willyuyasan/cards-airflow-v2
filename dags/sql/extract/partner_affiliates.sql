select
    aff.affiliate_id,
    aff.affiliate_type,
    aff.account_id,
    ifnull(acc.account_type,'') as account_type,
    aff.in_house,
    aff.first_name,
    aff.last_name,
    aff.company_name,
    replace(aff.city, char(9), ' ') as city,
    aff.state,
    replace(aff.street, char(9), ' ') as street,
    ifnull(aff.address2,''),
    aff.country,
    aff.zip,
    aff.status,
    aff.time_inserted,
    aff.time_modified,
    aff.deleted,
    aff.ref_id
from cccomus.partner_affiliates aff
left join cccomus.partner_accounts acc on (acc.account_id = aff.account_id);