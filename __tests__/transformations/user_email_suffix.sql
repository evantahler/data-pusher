alter table users add column emailsuffix text;
update users set emailsuffix = substring(email from '@(.*)$');
