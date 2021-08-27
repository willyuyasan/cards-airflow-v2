use cms;
UPDATE cs_users SET password = 'acbd18db4cc2f85cedef654fccc4a4d8';
UPDATE cs_settings SET value = '/var/www/html/build/' WHERE `key` = 'CMS_publish';