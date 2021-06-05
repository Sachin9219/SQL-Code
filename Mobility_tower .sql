
create table sbx_falcon.sachin_tower_JABO_1 as
select distinct msc from amp.xqi_gcell_4g_w WHERE regional_10 in ('JABO 1');
4167


create table sbx_falcon.sachin_tower_home_wkd_aug as
select tower_home_wkd,year,month,count(msisdn) as home_user from prd_bca.phoenix_mob_m
where month=202008
group by tower_home_wkd,year,month;
34441

create table sbx_falcon.sachin_tower_home_wkd_sep as
select tower_home_wkd,year,month,count(msisdn) as home_user from prd_bca.phoenix_mob_m
where month=202009
group by tower_home_wkd,year,month;
34743


create table sbx_falcon.sachin_tower_work_wkd_oct as
select tower_work_wkd,year,month,count(msisdn) as home_user  from prd_bca.phoenix_mob_m
where month=202010
group by tower_home_wkd,year,month;
34875

create table sbx_falcon.sachin_tower_work_wkd_aug as
select tower_work_wkd,year,month,count(msisdn) as work_user from prd_bca.phoenix_mob_m
where month=202008
group by tower_work_wkd,year,month;
34490

create table sbx_falcon.sachin_tower_work_wkd_sep as
select tower_work_wkd,year,month,count(msisdn) as work_user from prd_bca.phoenix_mob_m
where month=202009
group by tower_work_wkd,year,month;
34798


create table sbx_falcon.sachin_tower_work_wkd_oct as
select tower_work_wkd,year,month,count(msisdn) as work_user from prd_bca.phoenix_mob_m
where month=202010
group by tower_work_wkd,year,month;
34931


create table sbx_falcon.sachin_tower_aug as
select aa.msc,bb.tower_home_wkd,cc.tower_work_wkd, 
bb.year,bb.month,bb.home_user,cc.work_user
from sbx_falcon.sachin_tower_JABO_1 as aa
left join (select distinct * from sbx_falcon.sachin_tower_home_wkd_aug) as bb on aa.msc=bb.tower_home_wkd
left join (select distinct * from sbx_falcon.sachin_tower_work_wkd_aug) as cc on aa.msc=cc.tower_work_wkd
4167


create table sbx_falcon.sachin_tower_aug1 as
select msc,tower_home_wkd,tower_work_wkd, 
year,month,home_user,work_user,(home_user+work_user) as total_user
from sbx_falcon.sachin_tower_aug



create table sbx_falcon.sachin_tower_sep as
select aa.msc,bb.tower_home_wkd,cc.tower_work_wkd, 
bb.year,bb.month,bb.home_user,cc.work_user
from sbx_falcon.sachin_tower_JABO_1 as aa
left join (select distinct * from sbx_falcon.sachin_tower_home_wkd_sep) as bb on aa.msc=bb.tower_home_wkd
left join (select distinct * from sbx_falcon.sachin_tower_work_wkd_sep) as cc on aa.msc=cc.tower_work_wkd;
4167

create table sbx_falcon.sachin_tower_sep1 as
select msc,tower_home_wkd,tower_work_wkd, 
year,month,home_user,work_user,(home_user+work_user) as total_user
from sbx_falcon.sachin_tower_sep



create table sbx_falcon.sachin_tower_oct as
select aa.msc,bb.tower_home_wkd,cc.tower_work_wkd, 
bb.year,bb.month,bb.home_user,cc.work_user
from sbx_falcon.sachin_tower_JABO_1 as aa
left join (select distinct * from sbx_falcon.sachin_tower_home_wkd_oct) as bb on aa.msc=bb.tower_home_wkd
left join (select distinct * from sbx_falcon.sachin_tower_work_wkd_oct) as cc on aa.msc=cc.tower_work_wkd;
4167

create table sbx_falcon.sachin_tower_oct1 as
select msc,tower_home_wkd,tower_work_wkd, 
year,month,home_user,work_user,(home_user+work_user) as total_user
from sbx_falcon.sachin_tower_oct

