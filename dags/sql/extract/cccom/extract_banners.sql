select br.banner_id,
 br.banner_affiliate_type as affiliate_type,
 br.banner_type,
 br.name,
 br.destination_url as url,
 br.deleted
from cccomus.partner_banners br;
