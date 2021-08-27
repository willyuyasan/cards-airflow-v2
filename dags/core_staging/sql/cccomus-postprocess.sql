use cccomus;


-- -----------------------------------------------------------
-- creating before Insert trigger on Transactions_click
-- -----------------------------------------------------------

drop trigger if exists new_click;

delimiter |

CREATE TRIGGER `new_click` AFTER INSERT ON `transactions_click`
FOR EACH ROW
BEGIN
    -- the following session variable is used for disabling the trigger temporarily in a session
    -- normally if this session variable is not set , then trigger will operate normally
    -- DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SELECT 'Error occured';

    if @disable_tc_ainsert_trigger is NULL or @disable_tc_ainsert_trigger = 1 then

                   INSERT IGNORE INTO transactions_affiliate (transaction_id,website_id,product_creative_id,date_inserted)
                                VALUES (NEW.tracking_id,NEW.website_id,NEW.product_id,NEW.date_inserted);

                   INSERT IGNORE INTO transactions_click_external
                      (
                                        click_id,
                                        tracking_id,
                                        affiliate_id,
                                        website_id,
                                        product_id,
                                        product_name,
                                        product_type_id,
                                        program_id,
                                        creative_id,
                                        ip_address,
                                        referer_url,
                                        user_variable,
                                        user_agent,
                                        exit_page_id,
                                        page_position,
                                        content_type_id,
                                        date_inserted,
                                        tenant_id
                          )
                                SELECT
                                        NEW.click_id,
                                        NEW.tracking_id,
                                        NEW.affiliate_id,
                                        NEW.website_id,
                                        NEW.product_id,
                                        cards.cardTitle,
                                        cards.product_type_id,
                                        cards.program_id,
                                        NEW.creative_id,
                                        NEW.ip_address,
                                        NEW.referer_url,
                                        NEW.user_variable,
                                        NEW.user_agent,
                                        NEW.exit_page_id,
                                        NEW.page_position,
                                        NEW.content_type_id,
                                        NEW.date_inserted,
                                        NEW.tenant_id
                                FROM partner_websites AS pw
                                        JOIN partner_affiliates AS pa ON pa.affiliate_id = pw.affiliate_id
                                        JOIN cms_cards AS cards ON cards.cardId = NEW.product_id
                                WHERE pw.website_id = NEW.website_id;
    end if;
END
|

delimiter ;




-- -----------------------------------------------------------------
-- following views gets created as table everytime refresh job runs
-- this issue started during Data Minimization initiative work
-- This is the patch work to fix the issue . BIA-3103
-- -----------------------------------------------------------------
drop table if exists cccomus.card_boost;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`card_boost` AS select `cms`.`card_boost`.`card_id` AS `card_id`,`cms`.`card_boost`.`boost` AS `boost` from `cms`.`card_boost` ;

drop table if exists cccomus.cms_alternate_links;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_alternate_links` AS select `cms`.`alternate_links`.`affiliate_id` AS `affiliate_id`,`cms`.`alternate_links`.`clickable_id` AS `clickable_id`,`cms`.`alternate_links`.`url` AS `url`,`cms`.`alternate_links`.`website_id` AS `website_id` from `cms`.`alternate_links` ;

drop table if exists cccomus.cms_card_categories;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_categories` AS select `cms`.`card_categories`.`card_category_id` AS `card_category_id`,`cms`.`card_categories`.`card_category_name` AS `card_category_name`,`cms`.`card_categories`.`card_category_display_name` AS `card_category_display_name`,`cms`.`card_categories`.`deleted` AS `deleted` from `cms`.`card_categories` ;

drop table if exists cccomus.cms_card_category_contexts;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_category_contexts` AS select `cms`.`card_category_contexts`.`card_category_context_id` AS `card_category_context_id`,`cms`.`card_category_contexts`.`card_category_context_name` AS `card_category_context_name` from `cms`.`card_category_contexts` ;

drop table if exists cccomus.cms_card_category_group_to_category;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_category_group_to_category` AS select `cms`.`card_category_group_to_category`.`card_category_group_id` AS `card_category_group_id`,`cms`.`card_category_group_to_category`.`card_category_id` AS `card_category_id`,`cms`.`card_category_group_to_category`.`card_category_group_rank` AS `card_category_group_rank` from `cms`.`card_category_group_to_category` ;

drop table if exists cccomus.cms_card_category_groups;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_category_groups` AS select `cms`.`card_category_groups`.`id` AS `id`,`cms`.`card_category_groups`.`card_category_group_name` AS `card_category_group_name`,`cms`.`card_category_groups`.`context_id` AS `context_id`,`cms`.`card_category_groups`.`inserted` AS `inserted`,`cms`.`card_category_groups`.`updated` AS `updated` from `cms`.`card_category_groups` ;

drop table if exists cccomus.cms_card_category_ranks;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_category_ranks` AS select `cms`.`card_category_ranks`.`card_category_rank_id` AS `card_category_rank_id`,`cms`.`card_category_ranks`.`card_category_rank` AS `card_category_rank`,`cms`.`card_category_ranks`.`card_category_context_id` AS `card_category_context_id`,`cms`.`card_category_ranks`.`card_category_id` AS `card_category_id` from `cms`.`card_category_ranks` ;

drop table if exists cccomus.cms_card_data;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_data` AS select `cms`.`cs_carddata`.`cardId` AS `cardId`,`cms`.`cs_carddata`.`introApr` AS `introApr`,`cms`.`cs_carddata`.`introAprPeriod` AS `introAprPeriod`,`cms`.`cs_carddata`.`regularApr` AS `regularApr`,`cms`.`cs_carddata`.`annualFee` AS `annualFee`,`cms`.`cs_carddata`.`balanceTransfers` AS `balanceTransfers`,`cms`.`cs_carddata`.`balanceTransferFee` AS `balanceTransferFee`,`cms`.`cs_carddata`.`balanceTransferIntroApr` AS `balanceTransferIntroApr`,`cms`.`cs_carddata`.`balanceTransferIntroAprPeriod` AS `balanceTransferIntroAprPeriod`,`cms`.`cs_carddata`.`monthlyFee` AS `monthlyFee`,`cms`.`cs_carddata`.`creditNeeded` AS `creditNeeded`,`cms`.`cs_carddata`.`dateModified` AS `dateModified` from `cms`.`cs_carddata` ;

drop table if exists cccomus.cms_card_details;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_details` AS select `cms`.`rt_carddetails`.`id` AS `id`,`cms`.`rt_carddetails`.`cardShortName` AS `cardShortName`,`cms`.`rt_carddetails`.`cardLink` AS `cardLink`,`cms`.`rt_carddetails`.`appLink` AS `appLink`,`cms`.`rt_carddetails`.`cardDetailVersion` AS `cardDetailVersion`,`cms`.`rt_carddetails`.`cardDetailLabel` AS `cardDetailLabel`,`cms`.`rt_carddetails`.`cardId` AS `cardId`,`cms`.`rt_carddetails`.`campaignLink` AS `campaignLink`,`cms`.`rt_carddetails`.`cardPageMeta` AS `cardPageMeta`,`cms`.`rt_carddetails`.`cardDetailText` AS `cardDetailText`,`cms`.`rt_carddetails`.`cardIntroDetail` AS `cardIntroDetail`,`cms`.`rt_carddetails`.`cardMoreDetail` AS `cardMoreDetail`,`cms`.`rt_carddetails`.`cardSeeDetails` AS `cardSeeDetails`,`cms`.`rt_carddetails`.`categoryImage` AS `categoryImage`,`cms`.`rt_carddetails`.`categoryAltText` AS `categoryAltText`,`cms`.`rt_carddetails`.`cardIOImage` AS `cardIOImage`,`cms`.`rt_carddetails`.`cardIOAltText` AS `cardIOAltText`,`cms`.`rt_carddetails`.`cardButtonImage` AS `cardButtonImage`,`cms`.`rt_carddetails`.`cardButtonAltText` AS `cardButtonAltText`,`cms`.`rt_carddetails`.`cardIOButtonAltText` AS `cardIOButtonAltText`,`cms`.`rt_carddetails`.`cardIconSmall` AS `cardIconSmall`,`cms`.`rt_carddetails`.`cardIconMid` AS `cardIconMid`,`cms`.`rt_carddetails`.`cardIconLarge` AS `cardIconLarge`,`cms`.`rt_carddetails`.`detailOrder` AS `detailOrder`,`cms`.`rt_carddetails`.`dateCreated` AS `dateCreated`,`cms`.`rt_carddetails`.`dateUpdated` AS `dateUpdated`,`cms`.`rt_carddetails`.`fid` AS `fid`,`cms`.`rt_carddetails`.`cardListingString` AS `cardListingString`,`cms`.`rt_carddetails`.`cardPageHeaderString` AS `cardPageHeaderString`,`cms`.`rt_carddetails`.`imageAltText` AS `imageAltText`,`cms`.`rt_carddetails`.`active` AS `active`,`cms`.`rt_carddetails`.`deleted` AS `deleted`,`cms`.`rt_carddetails`.`specialsDescription` AS `specialsDescription`,`cms`.`rt_carddetails`.`specialsAdditionalLink` AS `specialsAdditionalLink`,`cms`.`rt_carddetails`.`cardTeaserText` AS `cardTeaserText` from `cms`.`rt_carddetails` ;

drop table if exists cccomus.cms_card_exclusion_map;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_exclusion_map` AS select `cms`.`cs_pagecardexclusionmap`.`mapid` AS `mapid`,`cms`.`cs_pagecardexclusionmap`.`siteid` AS `siteid`,`cms`.`cs_pagecardexclusionmap`.`pageid` AS `pageid`,`cms`.`cs_pagecardexclusionmap`.`cardid` AS `cardid` from `cms`.`cs_pagecardexclusionmap` ;

drop table if exists cccomus.cms_card_history;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_history` AS select `cms`.`cardhistory`.`campaigntype_id` AS `campaigntype_id`,`cms`.`cardhistory`.`dateinserted` AS `dateinserted`,`cms`.`cardhistory`.`campaigntype_name` AS `campaigntype_name`,`cms`.`cardhistory`.`intro_apr` AS `intro_apr`,`cms`.`cardhistory`.`delta_intro_apr` AS `delta_intro_apr`,`cms`.`cardhistory`.`intro_apr_movement` AS `intro_apr_movement`,`cms`.`cardhistory`.`intro_apr_period` AS `intro_apr_period`,`cms`.`cardhistory`.`delta_intro_apr_period` AS `delta_intro_apr_period`,`cms`.`cardhistory`.`intro_apr_period_movement` AS `intro_apr_period_movement`,`cms`.`cardhistory`.`regular_apr` AS `regular_apr`,`cms`.`cardhistory`.`delta_regular_apr` AS `delta_regular_apr`,`cms`.`cardhistory`.`regular_apr_movement` AS `regular_apr_movement` from `cms`.`cardhistory` ;

drop table if exists cccomus.cms_card_page_map;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_page_map` AS select `cms`.`rt_cardpagemap`.`cardcategorymapId` AS `cardcategorymapId`,`cms`.`rt_cardpagemap`.`pageInsert` AS `pageInsert`,`cms`.`rt_cardpagemap`.`cardpageId` AS `cardpageId`,`cms`.`rt_cardpagemap`.`cardId` AS `cardId`,`cms`.`rt_cardpagemap`.`rank` AS `rank` from `cms`.`rt_cardpagemap` ;

drop table if exists cccomus.cms_card_page_map_affiliate;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_page_map_affiliate` AS select `cms`.`rt_cardpagemap_affiliate`.`cardcategorymapId` AS `cardcategorymapId`,`cms`.`rt_cardpagemap_affiliate`.`pageInsert` AS `pageInsert`,`cms`.`rt_cardpagemap_affiliate`.`cardpageId` AS `cardpageId`,`cms`.`rt_cardpagemap_affiliate`.`cardId` AS `cardId`,`cms`.`rt_cardpagemap_affiliate`.`rank` AS `rank` from `cms`.`rt_cardpagemap_affiliate` ;

drop table if exists cccomus.cms_card_page_map_afwd;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_page_map_afwd` AS select `rt_cardpagemap_afwd`.`cardcategorymapId` AS `cardcategorymapId`,`rt_cardpagemap_afwd`.`pageInsert` AS `pageInsert`,`rt_cardpagemap_afwd`.`cardpageId` AS `cardpageId`,`rt_cardpagemap_afwd`.`cardId` AS `cardId`,`rt_cardpagemap_afwd`.`rank` AS `rank` from `cms`.`rt_cardpagemap_afwd` ;

drop table if exists cccomus.cms_card_page_map_bankrate;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_page_map_bankrate` AS select `cms`.`rt_cardpagemap_bankrate`.`cardcategorymapId` AS `cardcategorymapId`,`cms`.`rt_cardpagemap_bankrate`.`pageInsert` AS `pageInsert`,`cms`.`rt_cardpagemap_bankrate`.`cardpageId` AS `cardpageId`,`cms`.`rt_cardpagemap_bankrate`.`cardId` AS `cardId`,`cms`.`rt_cardpagemap_bankrate`.`rank` AS `rank` from `cms`.`rt_cardpagemap_bankrate` ;

drop table if exists cccomus.cms_card_page_map_cacc;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_page_map_cacc` AS (select `cms`.`rt_cardpagemap_cacc`.`cardcategorymapId` AS `cardcategorymapId`,`cms`.`rt_cardpagemap_cacc`.`pageInsert` AS `pageInsert`,`cms`.`rt_cardpagemap_cacc`.`cardpageId` AS `cardpageId`,`cms`.`rt_cardpagemap_cacc`.`cardId` AS `cardId`,`cms`.`rt_cardpagemap_cacc`.`rank` AS `rank` from `cms`.`rt_cardpagemap_cacc`) ;

drop table if exists cccomus.cms_card_page_map_ccg;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_page_map_ccg` AS select `cms`.`rt_cardpagemap_ccg`.`cardcategorymapId` AS `cardcategorymapId`,`cms`.`rt_cardpagemap_ccg`.`pageInsert` AS `pageInsert`,`cms`.`rt_cardpagemap_ccg`.`cardpageId` AS `cardpageId`,`cms`.`rt_cardpagemap_ccg`.`cardId` AS `cardId`,`cms`.`rt_cardpagemap_ccg`.`rank` AS `rank` from `cms`.`rt_cardpagemap_ccg` ;

drop table if exists cccomus.cms_card_page_map_cm;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_page_map_cm` AS (select `cms`.`rt_cardpagemap_cm`.`cardcategorymapId` AS `cardcategorymapId`,`cms`.`rt_cardpagemap_cm`.`pageInsert` AS `pageInsert`,`cms`.`rt_cardpagemap_cm`.`cardpageId` AS `cardpageId`,`cms`.`rt_cardpagemap_cm`.`cardId` AS `cardId`,`cms`.`rt_cardpagemap_cm`.`rank` AS `rank` from `cms`.`rt_cardpagemap_cm`) ;

drop table if exists cccomus.cms_card_page_map_frca;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_page_map_frca` AS (select `cms`.`rt_cardpagemap_frca`.`cardcategorymapId` AS `cardcategorymapId`,`cms`.`rt_cardpagemap_frca`.`pageInsert` AS `pageInsert`,`cms`.`rt_cardpagemap_frca`.`cardpageId` AS `cardpageId`,`cms`.`rt_cardpagemap_frca`.`cardId` AS `cardId`,`cms`.`rt_cardpagemap_frca`.`rank` AS `rank` from `cms`.`rt_cardpagemap_frca`) ;

drop table if exists cccomus.cms_card_page_map_tarjetas;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_page_map_tarjetas` AS (select `cms`.`rt_cardpagemap_tarjetas`.`cardcategorymapId` AS `cardcategorymapId`,`cms`.`rt_cardpagemap_tarjetas`.`pageInsert` AS `pageInsert`,`cms`.`rt_cardpagemap_tarjetas`.`cardpageId` AS `cardpageId`,`cms`.`rt_cardpagemap_tarjetas`.`cardId` AS `cardId`,`cms`.`rt_cardpagemap_tarjetas`.`rank` AS `rank` from `cms`.`rt_cardpagemap_tarjetas`) ;

drop table if exists cccomus.cms_card_page_positions;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_page_positions` AS (select `cms`.`card_page_positions`.`id` AS `id`,`cms`.`card_page_positions`.`cardpageId` AS `cardpageId`,`cms`.`card_page_positions`.`cardId` AS `cardId`,`cms`.`card_page_positions`.`websiteId` AS `websiteId`,`cms`.`card_page_positions`.`rank` AS `rank`,`cms`.`card_page_positions`.`pageInsert` AS `pageInsert` from `cms`.`card_page_positions`) ;

drop table if exists cccomus.cms_card_placement_history;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_placement_history` AS select `cms`.`card_placement_history`.`cardpageId` AS `cardpageId`,`cms`.`card_placement_history`.`cardId` AS `cardId`,`cms`.`card_placement_history`.`rank` AS `rank`,`cms`.`card_placement_history`.`active` AS `active`,`cms`.`card_placement_history`.`time_snapped` AS `time_snapped` from `cms`.`card_placement_history` ;

drop table if exists cccomus.cms_card_ranks;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_card_ranks` AS select `cms`.`card_ranks`.`card_rank_id` AS `card_rank_id`,`cms`.`card_ranks`.`card_rank` AS `card_rank`,`cms`.`card_ranks`.`card_category_context_id` AS `card_category_context_id`,`cms`.`card_ranks`.`card_category_id` AS `card_category_id`,`cms`.`card_ranks`.`card_id` AS `card_id` from `cms`.`card_ranks` ;

drop table if exists cccomus.cms_cards;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_cards` AS select `cms`.`rt_cards`.`id` AS `id`,`cms`.`rt_cards`.`cardId` AS `cardId`,`cms`.`rt_cards`.`site_code` AS `site_code`,`cms`.`rt_cards`.`cardTitle` AS `cardTitle`,`cms`.`rt_cards`.`cardDescription` AS `cardDescription`,`cms`.`rt_cards`.`merchant` AS `merchant`,`cms`.`rt_cards`.`program_id` AS `program_id`,`cms`.`rt_cards`.`introApr` AS `introApr`,`cms`.`rt_cards`.`active_introApr` AS `active_introApr`,`cms`.`rt_cards`.`introAprPeriod` AS `introAprPeriod`,`cms`.`rt_cards`.`active_introAprPeriod` AS `active_introAprPeriod`,`cms`.`rt_cards`.`regularApr` AS `regularApr`,`cms`.`rt_cards`.`variable` AS `variable`,`cms`.`rt_cards`.`active_regularApr` AS `active_regularApr`,`cms`.`rt_cards`.`annualFee` AS `annualFee`,`cms`.`rt_cards`.`active_annualFee` AS `active_annualFee`,`cms`.`rt_cards`.`monthlyFee` AS `monthlyFee`,`cms`.`rt_cards`.`active_monthlyFee` AS `active_monthlyFee`,`cms`.`rt_cards`.`balanceTransfers` AS `balanceTransfers`,`cms`.`rt_cards`.`active_balanceTransfers` AS `active_balanceTransfers`,`cms`.`rt_cards`.`balanceTransferFee` AS `balanceTransferFee`,`cms`.`rt_cards`.`active_balanceTransferFee` AS `active_balanceTransferFee`,`cms`.`rt_cards`.`balanceTransferIntroApr` AS `balanceTransferIntroApr`,`cms`.`rt_cards`.`active_balanceTransferIntroApr` AS `active_balanceTransferIntroApr`,`cms`.`rt_cards`.`balanceTransferIntroAprPeriod` AS `balanceTransferIntroAprPeriod`,`cms`.`rt_cards`.`active_balanceTransferIntroAprPeriod` AS `active_balanceTransferIntroAprPeriod`,`cms`.`rt_cards`.`creditNeeded` AS `creditNeeded`,`cms`.`rt_cards`.`active_creditNeeded` AS `active_creditNeeded`,`cms`.`rt_cards`.`imagePath` AS `imagePath`,`cms`.`rt_cards`.`ratesAndFees` AS `ratesAndFees`,`cms`.`rt_cards`.`rewards` AS `rewards`,`cms`.`rt_cards`.`cardBenefits` AS `cardBenefits`,`cms`.`rt_cards`.`onlineServices` AS `onlineServices`,`cms`.`rt_cards`.`footNotes` AS `footNotes`,`cms`.`rt_cards`.`layout` AS `layout`,`cms`.`rt_cards`.`dateCreated` AS `dateCreated`,`cms`.`rt_cards`.`dateUpdated` AS `dateUpdated`,`cms`.`rt_cards`.`subCat` AS `subCat`,`cms`.`rt_cards`.`catTitle` AS `catTitle`,`cms`.`rt_cards`.`catDescription` AS `catDescription`,`cms`.`rt_cards`.`catImage` AS `catImage`,`cms`.`rt_cards`.`catImageAltText` AS `catImageAltText`,`cms`.`rt_cards`.`syndicate` AS `syndicate`,`cms`.`rt_cards`.`url` AS `url`,`cms`.`rt_cards`.`applyByPhoneNumber` AS `applyByPhoneNumber`,`cms`.`rt_cards`.`tPageText` AS `tPageText`,`cms`.`rt_cards`.`private` AS `private`,`cms`.`rt_cards`.`active` AS `active`,`cms`.`rt_cards`.`deleted` AS `deleted`,`cms`.`rt_cards`.`active_epd_pages` AS `active_epd_pages`,`cms`.`rt_cards`.`active_show_epd_rates` AS `active_show_epd_rates`,`cms`.`rt_cards`.`show_verify` AS `show_verify`,`cms`.`rt_cards`.`commission_label` AS `commission_label`,`cms`.`rt_cards`.`payout_cap` AS `payout_cap`,`cms`.`rt_cards`.`card_level_id` AS `card_level_id`,`cms`.`rt_cards`.`requires_approval` AS `requires_approval`,`cms`.`rt_cards`.`secured` AS `secured`,`cms`.`rt_cards`.`network_id` AS `network_id`,`cms`.`rt_cards`.`product_type_id` AS `product_type_id`,`cms`.`rt_cards`.`suppress_mobile` AS `suppress_mobile` from `cms`.`rt_cards` ;

drop table if exists cccomus.cms_default_network_keys;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_default_network_keys` AS select `cms`.`default_network_keys`.`network_id` AS `network_id`,`cms`.`default_network_keys`.`account_type` AS `account_type`,`cms`.`default_network_keys`.`default_network_key` AS `default_network_key`,`cms`.`default_network_keys`.`create_time` AS `create_time`,`cms`.`default_network_keys`.`update_time` AS `update_time` from `cms`.`default_network_keys` ;

drop table if exists cccomus.cms_device_types;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_device_types` AS select `cms`.`device_types`.`device_type_id` AS `device_type_id`,`cms`.`device_types`.`name` AS `name`,`cms`.`device_types`.`description` AS `description` from `cms`.`device_types` ;

drop table if exists cccomus.cms_link_types;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_link_types` AS select `cms`.`link_types`.`link_type_id` AS `link_type_id`,`cms`.`link_types`.`name` AS `name`,`cms`.`link_types`.`description` AS `description` from `cms`.`link_types` ;

drop table if exists cccomus.cms_merchant_service_details;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_merchant_service_details` AS select `cms`.`merchant_service_details`.`merchant_service_detail_id` AS `merchant_service_detail_id`,`cms`.`merchant_service_details`.`merchant_service_id` AS `merchant_service_id`,`cms`.`merchant_service_details`.`merchant_service_detail_version` AS `merchant_service_detail_version`,`cms`.`merchant_service_details`.`merchant_service_detail_label` AS `merchant_service_detail_label`,`cms`.`merchant_service_details`.`category_image_path` AS `category_image_path`,`cms`.`merchant_service_details`.`category_image_alt_text` AS `category_image_alt_text`,`cms`.`merchant_service_details`.`merchant_service_link` AS `merchant_service_link`,`cms`.`merchant_service_details`.`app_link` AS `app_link`,`cms`.`merchant_service_details`.`merchant_service_image_path` AS `merchant_service_image_path`,`cms`.`merchant_service_details`.`merchant_service_image_alt_text` AS `merchant_service_image_alt_text`,`cms`.`merchant_service_details`.`apply_button_alt_text` AS `apply_button_alt_text`,`cms`.`merchant_service_details`.`merchant_service_header_string` AS `merchant_service_header_string`,`cms`.`merchant_service_details`.`merchant_service_detail_text` AS `merchant_service_detail_text`,`cms`.`merchant_service_details`.`merchant_service_intro_detail` AS `merchant_service_intro_detail`,`cms`.`merchant_service_details`.`merchant_service_more_detail` AS `merchant_service_more_detail`,`cms`.`merchant_service_details`.`fid` AS `fid`,`cms`.`merchant_service_details`.`date_created` AS `date_created`,`cms`.`merchant_service_details`.`date_updated` AS `date_updated`,`cms`.`merchant_service_details`.`active` AS `active`,`cms`.`merchant_service_details`.`deleted` AS `deleted` from `cms`.`merchant_service_details` ;

drop table if exists cccomus.cms_merchant_services;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_merchant_services` AS select `cms`.`merchant_services`.`merchant_service_id` AS `merchant_service_id`,`cms`.`merchant_services`.`merchant_service_name` AS `merchant_service_name`,`cms`.`merchant_services`.`url` AS `url`,`cms`.`merchant_services`.`description` AS `description`,`cms`.`merchant_services`.`setup_fee` AS `setup_fee`,`cms`.`merchant_services`.`active_setup_fee` AS `active_setup_fee`,`cms`.`merchant_services`.`monthly_minimum` AS `monthly_minimum`,`cms`.`merchant_services`.`active_monthly_minimum` AS `active_monthly_minimum`,`cms`.`merchant_services`.`gateway_fee` AS `gateway_fee`,`cms`.`merchant_services`.`active_gateway_fee` AS `active_gateway_fee`,`cms`.`merchant_services`.`statement_fee` AS `statement_fee`,`cms`.`merchant_services`.`active_statement_fee` AS `active_statement_fee`,`cms`.`merchant_services`.`discount_rate` AS `discount_rate`,`cms`.`merchant_services`.`active_discount_rate` AS `active_discount_rate`,`cms`.`merchant_services`.`transaction_fee` AS `transaction_fee`,`cms`.`merchant_services`.`active_transaction_fee` AS `active_transaction_fee`,`cms`.`merchant_services`.`tech_support_fee` AS `tech_support_fee`,`cms`.`merchant_services`.`active_tech_support_fee` AS `active_tech_support_fee`,`cms`.`merchant_services`.`date_created` AS `date_created`,`cms`.`merchant_services`.`date_updated` AS `date_updated`,`cms`.`merchant_services`.`active` AS `active`,`cms`.`merchant_services`.`deleted` AS `deleted` from `cms`.`merchant_services` ;

drop table if exists cccomus.cms_merchants;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_merchants` AS select `cms`.`cs_merchants`.`merchantid` AS `merchantid`,`cms`.`cs_merchants`.`merchantname` AS `merchantname`,`cms`.`cs_merchants`.`merchantcardpage` AS `merchantcardpage`,`cms`.`cs_merchants`.`default_payin_tier_id` AS `default_payin_tier_id`,`cms`.`cs_merchants`.`deleted` AS `deleted` from `cms`.`cs_merchants` ;

drop table if exists cccomus.cms_networks;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_networks` AS select `cms`.`networks`.`network_id` AS `network_id`,`cms`.`networks`.`name` AS `name`,`cms`.`networks`.`create_time` AS `create_time`,`cms`.`networks`.`update_time` AS `update_time`,`cms`.`networks`.`deleted` AS `deleted` from `cms`.`networks` ;

drop table if exists cccomus.cms_page_details;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_page_details` AS select `cms`.`rt_pagedetails`.`id` AS `id`,`cms`.`rt_pagedetails`.`pageDetailVersion` AS `pageDetailVersion`,`cms`.`rt_pagedetails`.`pageDetailLabel` AS `pageDetailLabel`,`cms`.`rt_pagedetails`.`cardpageId` AS `cardpageId`,`cms`.`rt_pagedetails`.`pageTitle` AS `pageTitle`,`cms`.`rt_pagedetails`.`pageIntroDescription` AS `pageIntroDescription`,`cms`.`rt_pagedetails`.`pageDescription` AS `pageDescription`,`cms`.`rt_pagedetails`.`pageSpecial` AS `pageSpecial`,`cms`.`rt_pagedetails`.`pageMeta` AS `pageMeta`,`cms`.`rt_pagedetails`.`pageLearnMore` AS `pageLearnMore`,`cms`.`rt_pagedetails`.`pageDisclaimer` AS `pageDisclaimer`,`cms`.`rt_pagedetails`.`pageHeaderImage` AS `pageHeaderImage`,`cms`.`rt_pagedetails`.`pageHeaderImageAltText` AS `pageHeaderImageAltText`,`cms`.`rt_pagedetails`.`pageSpecialOfferImage` AS `pageSpecialOfferImage`,`cms`.`rt_pagedetails`.`pageSpecialOfferImageAltText` AS `pageSpecialOfferImageAltText`,`cms`.`rt_pagedetails`.`pageSpecialOfferLink` AS `pageSpecialOfferLink`,`cms`.`rt_pagedetails`.`pageSmallImage` AS `pageSmallImage`,`cms`.`rt_pagedetails`.`pageSmallImageAltText` AS `pageSmallImageAltText`,`cms`.`rt_pagedetails`.`dateCreated` AS `dateCreated`,`cms`.`rt_pagedetails`.`dateUpdated` AS `dateUpdated`,`cms`.`rt_pagedetails`.`pageLink` AS `pageLink`,`cms`.`rt_pagedetails`.`fid` AS `fid`,`cms`.`rt_pagedetails`.`pageHeaderString` AS `pageHeaderString`,`cms`.`rt_pagedetails`.`primaryNavString` AS `primaryNavString`,`cms`.`rt_pagedetails`.`secondaryNavString` AS `secondaryNavString`,`cms`.`rt_pagedetails`.`topPickAltText` AS `topPickAltText`,`cms`.`rt_pagedetails`.`flagTopPick` AS `flagTopPick`,`cms`.`rt_pagedetails`.`flagAdditionalOffer` AS `flagAdditionalOffer`,`cms`.`rt_pagedetails`.`associatedArticleCategory` AS `associatedArticleCategory`,`cms`.`rt_pagedetails`.`articlesPerPage` AS `articlesPerPage`,`cms`.`rt_pagedetails`.`enableSort` AS `enableSort`,`cms`.`rt_pagedetails`.`itemsPerPage` AS `itemsPerPage`,`cms`.`rt_pagedetails`.`pageSeeAlso` AS `pageSeeAlso`,`cms`.`rt_pagedetails`.`siteMapDescription` AS `siteMapDescription`,`cms`.`rt_pagedetails`.`siteMapTitle` AS `siteMapTitle`,`cms`.`rt_pagedetails`.`active` AS `active`,`cms`.`rt_pagedetails`.`deleted` AS `deleted`,`cms`.`rt_pagedetails`.`sitemapLink` AS `sitemapLink`,`cms`.`rt_pagedetails`.`navBarString` AS `navBarString`,`cms`.`rt_pagedetails`.`subPageNav` AS `subPageNav`,`cms`.`rt_pagedetails`.`landingPage` AS `landingPage`,`cms`.`rt_pagedetails`.`landingPageFid` AS `landingPageFid`,`cms`.`rt_pagedetails`.`landingPageImage` AS `landingPageImage`,`cms`.`rt_pagedetails`.`landingPageHeaderString` AS `landingPageHeaderString`,`cms`.`rt_pagedetails`.`itemsOnFirstPage` AS `itemsOnFirstPage`,`cms`.`rt_pagedetails`.`showMainCatOnFirstPage` AS `showMainCatOnFirstPage` from `cms`.`rt_pagedetails` ;

drop table if exists cccomus.cms_page_site_map;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_page_site_map` AS select `cms`.`rt_pagecategorymap`.`categoryPageMapId` AS `categoryPageMapId`,`cms`.`rt_pagecategorymap`.`cardpageId` AS `cardpageId`,`cms`.`rt_pagecategorymap`.`categoryId` AS `categoryId`,`cms`.`rt_pagecategorymap`.`rank` AS `rank` from `cms`.`rt_pagecategorymap` ;

drop table if exists cccomus.cms_pages;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_pages` AS select `cms`.`rt_cardpages`.`cardpageId` AS `cardpageId`,`cms`.`rt_cardpages`.`pageName` AS `pageName`,`cms`.`rt_cardpages`.`pageType` AS `pageType`,`cms`.`rt_cardpages`.`contentType` AS `contentType`,`cms`.`rt_cardpages`.`alternate_tracking_flag` AS `alternate_tracking_flag`,`cms`.`rt_cardpages`.`schumerType` AS `schumerType`,`cms`.`rt_cardpages`.`dateCreated` AS `dateCreated`,`cms`.`rt_cardpages`.`dateUpdated` AS `dateUpdated`,`cms`.`rt_cardpages`.`type` AS `type`,`cms`.`rt_cardpages`.`active` AS `active`,`cms`.`rt_cardpages`.`deleted` AS `deleted`,`cms`.`rt_cardpages`.`rollup` AS `rollup`,`cms`.`rt_cardpages`.`card_category_id` AS `card_category_id`,`cms`.`rt_cardpages`.`active_introApr` AS `active_introApr`,`cms`.`rt_cardpages`.`active_introAprPeriod` AS `active_introAprPeriod`,`cms`.`rt_cardpages`.`active_regularApr` AS `active_regularApr`,`cms`.`rt_cardpages`.`active_annualFee` AS `active_annualFee`,`cms`.`rt_cardpages`.`active_monthlyFee` AS `active_monthlyFee`,`cms`.`rt_cardpages`.`active_balanceTransfers` AS `active_balanceTransfers`,`cms`.`rt_cardpages`.`active_balanceTransferFee` AS `active_balanceTransferFee`,`cms`.`rt_cardpages`.`active_balanceTransferIntroApr` AS `active_balanceTransferIntroApr`,`cms`.`rt_cardpages`.`active_balanceTransferIntroAprPeriod` AS `active_balanceTransferIntroAprPeriod`,`cms`.`rt_cardpages`.`active_creditNeeded` AS `active_creditNeeded`,`cms`.`rt_cardpages`.`active_transactionFeeSignature` AS `active_transactionFeeSignature`,`cms`.`rt_cardpages`.`active_transactionFeePin` AS `active_transactionFeePin`,`cms`.`rt_cardpages`.`active_atmFee` AS `active_atmFee`,`cms`.`rt_cardpages`.`active_prepaidText` AS `active_prepaidText`,`cms`.`rt_cardpages`.`active_loadFee` AS `active_loadFee`,`cms`.`rt_cardpages`.`active_activationFee` AS `active_activationFee` from `cms`.`rt_cardpages` ;

drop table if exists cccomus.cms_product_links;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_product_links` AS select `cms`.`product_links`.`link_id` AS `link_id`,`cms`.`product_links`.`product_id` AS `product_id`,`cms`.`product_links`.`link_type_id` AS `link_type_id`,`cms`.`product_links`.`device_type_id` AS `device_type_id`,`cms`.`product_links`.`website_id` AS `website_id`,`cms`.`product_links`.`account_type_id` AS `account_type_id`,`cms`.`product_links`.`url` AS `url`,`cms`.`product_links`.`date_created` AS `date_created`,`cms`.`product_links`.`date_updated` AS `date_updated`,`cms`.`product_links`.`updated_by` AS `updated_by` from `cms`.`product_links` ;

drop table if exists cccomus.cms_product_types;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_product_types` AS (select `cms`.`product_types`.`product_type_id` AS `product_type_id`,`cms`.`product_types`.`product_type_name` AS `product_type_name`,`cms`.`product_types`.`date_inserted` AS `date_inserted`,`cms`.`product_types`.`deleted` AS `deleted` from `cms`.`product_types`) ;

drop table if exists cccomus.cms_programs;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_programs` AS select `cms`.`programs`.`program_id` AS `program_id`,`cms`.`programs`.`program_name` AS `program_name`,`cms`.`programs`.`issuer_id` AS `issuer_id`,`cms`.`programs`.`program_default` AS `program_default`,`cms`.`programs`.`date_created` AS `date_created`,`cms`.`programs`.`deleted` AS `deleted`,`cms`.`programs`.`private` AS `private` from `cms`.`programs` ;

drop table if exists cccomus.cms_secured_merchants;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_secured_merchants` AS (select `cms`.`cs_secured_merchants`.`sercured_merchant_id` AS `sercured_merchant_id`,`cms`.`cs_secured_merchants`.`merchant_id` AS `merchant_id`,`cms`.`cs_secured_merchants`.`insulator_class` AS `insulator_class` from `cms`.`cs_secured_merchants`) ;

drop table if exists cccomus.cms_sites;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_sites` AS (select `cms`.`rt_sites`.`siteId` AS `siteId`,`cms`.`rt_sites`.`siteName` AS `siteName`,`cms`.`rt_sites`.`siteTitle` AS `siteTitle`,`cms`.`rt_sites`.`siteDescription` AS `siteDescription`,`cms`.`rt_sites`.`language` AS `language`,`cms`.`rt_sites`.`layout` AS `layout`,`cms`.`rt_sites`.`pagetype` AS `pagetype`,`cms`.`rt_sites`.`applyLogo` AS `applyLogo`,`cms`.`rt_sites`.`ftpSite` AS `ftpSite`,`cms`.`rt_sites`.`ftpPath` AS `ftpPath`,`cms`.`rt_sites`.`sourcePath` AS `sourcePath`,`cms`.`rt_sites`.`corePath` AS `corePath`,`cms`.`rt_sites`.`publishPath` AS `publishPath`,`cms`.`rt_sites`.`publishurl` AS `publishurl`,`cms`.`rt_sites`.`hostname` AS `hostname`,`cms`.`rt_sites`.`postBuildScript` AS `postBuildScript`,`cms`.`rt_sites`.`publishScript` AS `publishScript`,`cms`.`rt_sites`.`order` AS `order`,`cms`.`rt_sites`.`dateCreated` AS `dateCreated`,`cms`.`rt_sites`.`dateUpdated` AS `dateUpdated`,`cms`.`rt_sites`.`dateLastBuilt` AS `dateLastBuilt`,`cms`.`rt_sites`.`dblocation` AS `dblocation`,`cms`.`rt_sites`.`dbname` AS `dbname`,`cms`.`rt_sites`.`ccbuildPublish` AS `ccbuildPublish`,`cms`.`rt_sites`.`individualcards` AS `individualcards`,`cms`.`rt_sites`.`individualcarddir` AS `individualcarddir`,`cms`.`rt_sites`.`individualmerchantservices` AS `individualmerchantservices`,`cms`.`rt_sites`.`individualmerchantservicesdir` AS `individualmerchantservicesdir`,`cms`.`rt_sites`.`createSeoDoc` AS `createSeoDoc`,`cms`.`rt_sites`.`sitemap` AS `sitemap`,`cms`.`rt_sites`.`active` AS `active`,`cms`.`rt_sites`.`deleted` AS `deleted`,`cms`.`rt_sites`.`articledb` AS `articledb`,`cms`.`rt_sites`.`articledbhost` AS `articledbhost`,`cms`.`rt_sites`.`articletableprefix` AS `articletableprefix`,`cms`.`rt_sites`.`articledbun` AS `articledbun`,`cms`.`rt_sites`.`articledbpw` AS `articledbpw`,`cms`.`rt_sites`.`articleindexlink` AS `articleindexlink`,`cms`.`rt_sites`.`sitemaplink` AS `sitemaplink`,`cms`.`rt_sites`.`articleSiteMapFile` AS `articleSiteMapFile`,`cms`.`rt_sites`.`yahooArticleFile` AS `yahooArticleFile`,`cms`.`rt_sites`.`yahooArticleCategoryFile` AS `yahooArticleCategoryFile`,`cms`.`rt_sites`.`googleArticleFile` AS `googleArticleFile`,`cms`.`rt_sites`.`dbun` AS `dbun`,`cms`.`rt_sites`.`dbpw` AS `dbpw`,`cms`.`rt_sites`.`landingPageDir` AS `landingPageDir`,`cms`.`rt_sites`.`editorialLandingPgPath` AS `editorialLandingPgPath`,`cms`.`rt_sites`.`creditCardNewsPg` AS `creditCardNewsPg`,`cms`.`rt_sites`.`alternativecardpages` AS `alternativecardpages`,`cms`.`rt_sites`.`alternativecardpagesdir` AS `alternativecardpagesdir`,`cms`.`rt_sites`.`version_id` AS `version_id` from `cms`.`rt_sites`) ;

drop table if exists cccomus.cms_subpage_page_map;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_subpage_page_map` AS select `cms`.`rt_pagesubpagemap`.`mapid` AS `mapid`,`cms`.`rt_pagesubpagemap`.`siteid` AS `siteid`,`cms`.`rt_pagesubpagemap`.`masterpageid` AS `masterpageid`,`cms`.`rt_pagesubpagemap`.`subpageid` AS `subpageid`,`cms`.`rt_pagesubpagemap`.`hide` AS `hide`,`cms`.`rt_pagesubpagemap`.`rank` AS `rank` from `cms`.`rt_pagesubpagemap` ;

drop table if exists cccomus.cms_subpage_page_map_affiliate;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_subpage_page_map_affiliate` AS select `cms`.`rt_pagesubpagemap_affiliate`.`mapid` AS `mapid`,`cms`.`rt_pagesubpagemap_affiliate`.`siteid` AS `siteid`,`cms`.`rt_pagesubpagemap_affiliate`.`masterpageid` AS `masterpageid`,`cms`.`rt_pagesubpagemap_affiliate`.`subpageid` AS `subpageid`,`cms`.`rt_pagesubpagemap_affiliate`.`hide` AS `hide`,`cms`.`rt_pagesubpagemap_affiliate`.`rank` AS `rank` from `cms`.`rt_pagesubpagemap_affiliate` ;

drop table if exists cccomus.cms_subpage_page_map_afwd;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_subpage_page_map_afwd` AS select `rt_pagesubpagemap_afwd`.`mapid` AS `mapid`,`rt_pagesubpagemap_afwd`.`siteid` AS `siteid`,`rt_pagesubpagemap_afwd`.`masterpageid` AS `masterpageid`,`rt_pagesubpagemap_afwd`.`subpageid` AS `subpageid`,`rt_pagesubpagemap_afwd`.`hide` AS `hide`,`rt_pagesubpagemap_afwd`.`rank` AS `rank` from `cms`.`rt_pagesubpagemap_afwd` ;

drop table if exists cccomus.cms_subpage_page_map_bankrate;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_subpage_page_map_bankrate` AS select `cms`.`rt_pagesubpagemap_bankrate`.`mapid` AS `mapid`,`cms`.`rt_pagesubpagemap_bankrate`.`siteid` AS `siteid`,`cms`.`rt_pagesubpagemap_bankrate`.`masterpageid` AS `masterpageid`,`cms`.`rt_pagesubpagemap_bankrate`.`subpageid` AS `subpageid`,`cms`.`rt_pagesubpagemap_bankrate`.`hide` AS `hide`,`cms`.`rt_pagesubpagemap_bankrate`.`rank` AS `rank` from `cms`.`rt_pagesubpagemap_bankrate` ;

drop table if exists cccomus.cms_subpage_page_map_cacc;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_subpage_page_map_cacc` AS (select `cms`.`rt_pagesubpagemap_cacc`.`mapid` AS `mapid`,`cms`.`rt_pagesubpagemap_cacc`.`siteid` AS `siteid`,`cms`.`rt_pagesubpagemap_cacc`.`masterpageid` AS `masterpageid`,`cms`.`rt_pagesubpagemap_cacc`.`subpageid` AS `subpageid`,`cms`.`rt_pagesubpagemap_cacc`.`hide` AS `hide`,`cms`.`rt_pagesubpagemap_cacc`.`rank` AS `rank` from `cms`.`rt_pagesubpagemap_cacc`) ;

drop table if exists cccomus.cms_subpage_page_map_ccg;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_subpage_page_map_ccg` AS select `cms`.`rt_pagesubpagemap_ccg`.`mapid` AS `mapid`,`cms`.`rt_pagesubpagemap_ccg`.`siteid` AS `siteid`,`cms`.`rt_pagesubpagemap_ccg`.`masterpageid` AS `masterpageid`,`cms`.`rt_pagesubpagemap_ccg`.`subpageid` AS `subpageid`,`cms`.`rt_pagesubpagemap_ccg`.`hide` AS `hide`,`cms`.`rt_pagesubpagemap_ccg`.`rank` AS `rank` from `cms`.`rt_pagesubpagemap_ccg` ;

drop table if exists cccomus.cms_subpage_page_map_cm;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_subpage_page_map_cm` AS (select `cms`.`rt_pagesubpagemap_cm`.`mapid` AS `mapid`,`cms`.`rt_pagesubpagemap_cm`.`siteid` AS `siteid`,`cms`.`rt_pagesubpagemap_cm`.`masterpageid` AS `masterpageid`,`cms`.`rt_pagesubpagemap_cm`.`subpageid` AS `subpageid`,`cms`.`rt_pagesubpagemap_cm`.`hide` AS `hide`,`cms`.`rt_pagesubpagemap_cm`.`rank` AS `rank` from `cms`.`rt_pagesubpagemap_cm`) ;

drop table if exists cccomus.cms_subpage_page_map_frca;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_subpage_page_map_frca` AS (select `cms`.`rt_pagesubpagemap_frca`.`mapid` AS `mapid`,`cms`.`rt_pagesubpagemap_frca`.`siteid` AS `siteid`,`cms`.`rt_pagesubpagemap_frca`.`masterpageid` AS `masterpageid`,`cms`.`rt_pagesubpagemap_frca`.`subpageid` AS `subpageid`,`cms`.`rt_pagesubpagemap_frca`.`hide` AS `hide`,`cms`.`rt_pagesubpagemap_frca`.`rank` AS `rank` from `cms`.`rt_pagesubpagemap_frca`) ;

drop table if exists cccomus.cms_subpage_page_map_tarjetas;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_subpage_page_map_tarjetas` AS (select `cms`.`rt_pagesubpagemap_tarjetas`.`mapid` AS `mapid`,`cms`.`rt_pagesubpagemap_tarjetas`.`siteid` AS `siteid`,`cms`.`rt_pagesubpagemap_tarjetas`.`masterpageid` AS `masterpageid`,`cms`.`rt_pagesubpagemap_tarjetas`.`subpageid` AS `subpageid`,`cms`.`rt_pagesubpagemap_tarjetas`.`hide` AS `hide`,`cms`.`rt_pagesubpagemap_tarjetas`.`rank` AS `rank` from `cms`.`rt_pagesubpagemap_tarjetas`) ;

drop table if exists cccomus.cms_versions;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`cms_versions` AS select `cms`.`versions`.`version_id` AS `version_id`,`cms`.`versions`.`version_name` AS `version_name`,`cms`.`versions`.`version_description` AS `version_description`,`cms`.`versions`.`deleted` AS `deleted`,`cms`.`versions`.`insert_time` AS `insert_time`,`cms`.`versions`.`update_time` AS `update_time` from `cms`.`versions` ;

drop table if exists cccomus.latest_issuer_process_date;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`latest_issuer_process_date` AS select `m`.`merchantid` AS `merchant_id`,`m`.`merchantname` AS `merchant_name`,str_to_date(date_format(max(`tr`.`provider_process_date`),'%Y-%m-%d'),'%Y-%m-%d %h:%i:%s') AS `latest_process_time` from ((`cccomus`.`partner_affiliate_sales_transactions_report` `tr` join `cccomus`.`cms_cards` `c` on((`tr`.`card_id` = `c`.`cardId`))) join `cccomus`.`cms_merchants` `m` on((`c`.`merchant` = `m`.`merchantid`))) where (`tr`.`provider_process_date` > (now() - interval 60 day)) group by `m`.`merchantid` order by `m`.`merchantname` ;

drop table if exists cccomus.page_subpage_ranking;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`page_subpage_ranking` AS select `pd`.`cardpageId` AS `page_id`,`pd`.`fid` AS `fid`,-(1) AS `sub_page_rank` from (`cccomus`.`cms_page_site_map` `psm` join `cccomus`.`cms_page_details` `pd` on((`psm`.`cardpageId` = `pd`.`cardpageId`))) where ((`pd`.`pageDetailVersion` = -(1)) and (`psm`.`categoryId` = 35)) union select `spm`.`subpageid` AS `page_id`,`pd`.`fid` AS `fid`,`spm`.`rank` AS `sub_page_rank` from (`cccomus`.`cms_subpage_page_map` `spm` join `cccomus`.`cms_page_details` `pd` on((`spm`.`masterpageid` = `pd`.`cardpageId`))) where ((`pd`.`pageDetailVersion` = -(1)) and (`spm`.`siteid` = 35)) ;

drop table if exists cccomus.partner_affiliate_click_reporting;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`partner_affiliate_click_reporting` AS select `cl`.`affiliate_id` AS `affiliate_id`,`cl`.`website_id` AS `website_id`,`cl`.`card_id` AS `card_id`,`cl`.`date` AS `date_inserted`,ifnull(`co`.`approvalSalesCount`,0) AS `saleClickCount`,'0' AS `nonSaleClickCount`,ifnull(`co`.`applicationSalesCount`,0) AS `applicationsCount`,ifnull(`co`.`crossSaleCount`,0) AS `crossSaleCount`,ifnull(`cl`.`totalclicks`,0) AS `totalClicks`,ifnull(`co`.`totalcommission`,0) AS `totalCommission`,ifnull(`co`.`totalAdjustment`,0) AS `totalAdjustment`,'0' AS `nonSalesClickCommission`,'0' AS `salesClickCommission` from (`cccomus`.`click_summary` `cl` left join `cccomus`.`commission_summary` `co` on(((`co`.`affiliate_id` = `cl`.`affiliate_id`) and (`co`.`card_id` = `cl`.`card_id`) and (`co`.`date` = `cl`.`date`) and (`co`.`website_id` = `cl`.`website_id`)))) ;

drop table if exists cccomus.partner_affiliate_click_reporting_internal;
Create or replace ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `cccomus`.`partner_affiliate_click_reporting_internal` AS select `cl`.`affiliate_id` AS `affiliate_id`,`cl`.`website_id` AS `website_id`,`cl`.`card_id` AS `card_id`,`cl`.`date` AS `date_inserted`,ifnull(`co`.`approvalSalesCount`,0) AS `saleClickCount`,'0' AS `nonSaleClickCount`,ifnull(`co`.`applicationSalesCount`,0) AS `applicationsCount`,ifnull(`co`.`crossSaleCount`,0) AS `crossSaleCount`,ifnull(`cl`.`totalclicks`,0) AS `totalClicks`,ifnull(`co`.`totalcommission`,0) AS `totalCommission`,ifnull(`co`.`totalAdjustment`,0) AS `totalAdjustment`,ifnull(`rs`.`sales_rev`,0) AS `totalRevenue`,'0' AS `nonSalesClickCommission`,'0' AS `salesClickCommission` from ((`cccomus`.`click_summary` `cl` left join `cccomus`.`commission_summary` `co` on(((`co`.`affiliate_id` = `cl`.`affiliate_id`) and (`co`.`card_id` = `cl`.`card_id`) and (`co`.`date` = `cl`.`date`) and (`co`.`website_id` = `cl`.`website_id`)))) left join `cccomus`.`revenue_summary` `rs` on(((`rs`.`affiliate_id` = `cl`.`affiliate_id`) and (`rs`.`card_id` = `cl`.`card_id`) and (`rs`.`post_date` = `cl`.`date`) and (`rs`.`website_id` = `cl`.`website_id`)))) ;
