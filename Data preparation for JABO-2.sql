
## data prepration for Sep month for jabo-2 area

create table sbx_falcon.abhilash_leakage_im_Sep as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,
aa.year,aa.month,aa.day 
from prd_bca.dpi_usg_h_im_sdr as aa  inner join (select msc,replace(cgi,"-",".") as cgi_dot, '4g' as tech FROM amp.xqi_gcell_4g_w WHERE regional_10 in ('JABO 2') AND DAY = 20200919
union
select msc,replace(cgi,"-",".") as cgi, '3g' as tech FROM amp.xqi_gcell_3g_w WHERE region_10 in ('JABO 2') AND DAY = 20200919
) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 09
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,aa.year,
aa.month,aa.day;
 
create table sbx_falcon.abhilash_leakage_other_sep as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,
aa.year,aa.month,aa.day 
from prd_bca.dpi_usg_h_other_sdr as aa  inner join (select msc,replace(cgi,"-",".") as cgi_dot, '4g' as tech FROM amp.xqi_gcell_4g_w WHERE regional_10 in ('JABO 2') AND DAY = 20200919
union
select msc,replace(cgi,"-",".") as cgi, '3g' as tech FROM amp.xqi_gcell_3g_w WHERE region_10 in ('JABO 2') AND DAY = 20200919
) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 09
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,
aa.year,aa.month,aa.day;
 
create table sbx_falcon.abhilash_leakage_stream_sep as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,
aa.year,aa.month,aa.day 
from prd_bca.dpi_usg_h_stream_sdr as aa  inner join (select msc,replace(cgi,"-",".") as cgi_dot, '4g' as tech FROM amp.xqi_gcell_4g_w WHERE regional_10 in ('JABO 2') AND DAY = 20200919
union
select msc,replace(cgi,"-",".") as cgi, '3g' as tech FROM amp.xqi_gcell_3g_w WHERE region_10 in ('JABO 2') AND DAY = 20200919
) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 09
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,
aa.year,aa.month,aa.day;

create table sbx_falcon.abhilash_leakage_web_sep as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,
aa.year,aa.month,aa.day 
from prd_bca.dpi_usg_h_web_sdr as aa  inner join (select msc,replace(cgi,"-",".") as cgi_dot, '4g' as tech FROM amp.xqi_gcell_4g_w WHERE regional_10 in ('JABO 2') AND DAY = 20200919
union
select msc,replace(cgi,"-",".") as cgi, '3g' as tech FROM amp.xqi_gcell_3g_w WHERE region_10 in ('JABO 2') AND DAY = 20200919
) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 09
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,
aa.year,aa.month,aa.day;

create table sbx_falcon.abhilash_leakage_payload_sep as 
select * from sbx_falcon.abhilash_leakage_im_sep 
union all
select * from sbx_falcon.abhilash_leakage_other_sep
union all
select * from sbx_falcon.abhilash_leakage_stream_sep 
union all
select * from sbx_falcon.abhilash_leakage_web_sep

create table sbx_falcon.abhilash_leakage_analysis_sep as
select tech,msc,cgi,aa.subs_no,aa.msisdn,payload_mb,year,month,day from sbx_falcon.abhilash_leakage_payload_sep as aa
inner join (select subs_no,msisdn,tower_dmnt_dom_all from prd_bca.phoenix_mob_m where month = 202009) as bb on 
aa.msisdn = bb.msisdn and aa.subs_no = bb.subs_no and aa.msc = bb.tower_dmnt_dom_all;

create table sbx_falcon.abhilash_leakage_analysis_agg_sep_final_l1_monthly as
SELECT tech,msc,subs_no,msisdn,
round(sum(payload_mb),2) as sum_payload_mb,
count(distinct day) as count_day
FROM sbx_falcon.abhilash_leakage_analysis_sep
group by tech,msc,subs_no,msisdn;



## data prepration for Aug month for jabo-2 area

create table sbx_falcon.abhilash_leakage_im_Aug as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,
aa.year,aa.month,aa.day 
from prd_bca.dpi_usg_h_im_sdr as aa  inner join (select msc,replace(cgi,"-",".") as cgi_dot, '4g' as tech FROM amp.xqi_gcell_4g_w WHERE regional_10 in ('JABO 2') AND DAY = 20200919
union
select msc,replace(cgi,"-",".") as cgi, '3g' as tech FROM amp.xqi_gcell_3g_w WHERE region_10 in ('JABO 2') AND DAY = 20200919
) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 08
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,aa.year,
aa.month,aa.day;
 
create table sbx_falcon.abhilash_leakage_other_Aug as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,
aa.year,aa.month,aa.day 
from prd_bca.dpi_usg_h_other_sdr as aa  inner join (select msc,replace(cgi,"-",".") as cgi_dot, '4g' as tech FROM amp.xqi_gcell_4g_w WHERE regional_10 in ('JABO 2') AND DAY = 20200919
union
select msc,replace(cgi,"-",".") as cgi, '3g' as tech FROM amp.xqi_gcell_3g_w WHERE region_10 in ('JABO 2') AND DAY = 20200919
) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 08
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,
aa.year,aa.month,aa.day;

create table sbx_falcon.abhilash_leakage_stream_Aug as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,
aa.year,aa.month,aa.day 
from prd_bca.dpi_usg_h_stream_sdr as aa  inner join (select msc,replace(cgi,"-",".") as cgi_dot, '4g' as tech FROM amp.xqi_gcell_4g_w WHERE regional_10 in ('JABO 2') AND DAY = 20200919
union
select msc,replace(cgi,"-",".") as cgi, '3g' as tech FROM amp.xqi_gcell_3g_w WHERE region_10 in ('JABO 2') AND DAY = 20200919
) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 08
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,
aa.year,aa.month,aa.day;

create table sbx_falcon.abhilash_leakage_web_Aug as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,
aa.year,aa.month,aa.day 
from prd_bca.dpi_usg_h_web_sdr as aa  inner join (select msc,replace(cgi,"-",".") as cgi_dot, '4g' as tech FROM amp.xqi_gcell_4g_w WHERE regional_10 in ('JABO 2') AND DAY = 20200919
union
select msc,replace(cgi,"-",".") as cgi, '3g' as tech FROM amp.xqi_gcell_3g_w WHERE region_10 in ('JABO 2') AND DAY = 20200919
) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 08
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,
aa.year,aa.month,aa.day;

create table sbx_falcon.abhilash_leakage_payload_Aug as 
select * from sbx_falcon.abhilash_leakage_im_Aug
union all
select * from sbx_falcon.abhilash_leakage_other_Aug
union all
select * from sbx_falcon.abhilash_leakage_stream_Aug 
union all
select * from sbx_falcon.abhilash_leakage_web_Aug

create table sbx_falcon.abhilash_leakage_analysis_Aug1 as
select tech,msc,cgi,aa.subs_no,aa.msisdn,payload_mb,year,month,day from sbx_falcon.abhilash_leakage_payload_Aug as aa
inner join (select subs_no,msisdn,tower_dmnt_dom_all from prd_bca.phoenix_mob_m where month = 202008) as bb on 
aa.msisdn = bb.msisdn and aa.subs_no = bb.subs_no and aa.msc = bb.tower_dmnt_dom_all;

create table sbx_falcon.abhilash_leakage_analysis_agg_Aug_final_l1_monthly as
SELECT tech,msc,subs_no,msisdn,
round(sum(payload_mb),2) as sum_payload_mb,
count(distinct day) as count_day
FROM sbx_falcon.abhilash_leakage_analysis_Aug1
group by tech,msc,subs_no,msisdn;

