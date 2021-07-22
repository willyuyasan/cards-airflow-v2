select c.id,
c.site_code,
c.cardTitle,
c.cardDescription,
c.merchant,
ifnull(c.program_id,'') as program_id,
ifnull(cd.creditNeeded, '') as credit_needed_id,
c.syndicate,
c.private,
c.active,
c.deleted,
ifnull(c.commission_label,'') as commission_label,
ifnull(c.payout_cap, '') as payout_cap,
ifnull(c.card_level_id, '') as card_level_id,
c.requires_approval,
c.secured, 
ifnull(c.network_id,'') as network_id,
c.product_type_id,
c.suppress_mobile,
c.dateCreated,
c.dateUpdated,
ifnull(pc.time_created,'') as pc_time_created,
ifnull(pc.status,'') as pc_status,
ifnull(pc.deleted,'') as pc_deleted
from cccomus.cms_cards c
join cccomus.cms_card_data cd on (cd.cardId = c.cardId)
left join cccomus.partner_cards pc on (pc.card_id = c.cardid);