select cpcg.card_group_id group_id, 
cpcg.name group_name,
cpcg.deleted,
ifnull(cpcg.parent_group_id, '') as parent_group_id,
ifnull(ppcg.name, '') as parent_group_name,
ifnull(ppcg.deleted, '') as parent_group_deleted
from cccomus.partner_card_groups cpcg
left join cccomus.partner_card_groups ppcg on (ppcg.card_group_id = cpcg.parent_group_id);
