
create table sbx_falcon.Sachin_im_Sep2020 as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,SUM(total_events) as events,aa.year,
aa.month,aa.day 
from prd_bca.dpi_usg_h_im_sdr as aa  inner join (SELECT * from sbx_falcon.Sachin_500_MSC_data) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 9 and tech ='4g'
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,aa.year,aa.month,aa.day;

4554700

CREATE TABLE sbx_falcon.Sachin_IM_tower_dmnt_dom_Sep2020 AS
SELECT tech,msc,aa.subs_no,aa.msisdn,cgi,payload_mb,EVENTS,aa.year,aa.MONTH,aa.DAY
FROM sbx_falcon.Sachin_im_Sep2020 AS aa
INNER JOIN (SELECT tower_dmnt,msisdn,subs_no,day FROM prd_bca.phoenix_mob_d WHERE year =2020 and MONTH =9) AS bb 
ON aa.msc = bb.tower_dmnt
AND aa.msisdn = bb.msisdn
AND aa.subs_no = bb.subs_no
AND aa.day =bb.day;


2135758
CREATE TABLE sbx_falcon.Sachin_IM_tower_dmnt_agg_Sep2020 as
SELECT msc,year,MONTH,DAY,
       round(sum(payload_mb),2) AS total_payload_mb_IM_tower_dmnt,
       sum(EVENTS) AS total_events_IM_tower_dmnt
FROM sbx_falcon.Sachin_IM_tower_dmnt_dom_Sep2020
GROUP BY msc,year,MONTH,DAY;
13658 

CREATE TABLE sbx_falcon.Sachin_IM_Overall_agg_Sep2020 as
SELECT msc,year,MONTH,DAY,
       round(sum(payload_mb),2) AS total_payload_mb_IM_Overall,
       sum(EVENTS) AS total_events_IM_Overall
FROM sbx_falcon.Sachin_im_Sep2020
GROUP BY msc,year,MONTH,DAY;

13741

CREATE table Sachin_IM_tower_dmnt_Overall as
SELECT aa.msc,aa.year,aa.MONTH,aa.DAY,aa.total_payload_mb_IM_tower_dmnt,aa.total_events_IM_tower_dmnt,
bb.total_payload_mb_IM_Overall,bb.total_events_IM_Overall
from sbx_falcon.Sachin_IM_tower_dmnt_agg_Sep2020 as aa 
INNER JOIN (SELECT * from sbx_falcon.Sachin_IM_Overall_agg_Sep2020) as bb
on aa.msc = bb.msc and aa.year = aa.year and aa.month= bb.month AND aa.day =bb.day;

13658


create table sbx_falcon.Sachin_other_Sep2020 as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,SUM(total_events) as events,aa.year,
aa.month,aa.day 
from prd_bca.dpi_usg_h_other_sdr as aa  inner join (SELECT * from sbx_falcon.Sachin_500_MSC_data) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 9 and tech ='4g'
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,aa.year,aa.month,aa.day;

5504414


CREATE TABLE sbx_falcon.Sachin_other_tower_dmnt_Sep2020 AS
SELECT tech,msc,aa.subs_no,aa.msisdn,cgi,payload_mb,EVENTS,aa.year,aa.MONTH,aa.DAY
FROM sbx_falcon.Sachin_other_Sep2020 AS aa
INNER JOIN (SELECT tower_dmnt,msisdn,subs_no,day FROM prd_bca.phoenix_mob_d WHERE year =2020 and MONTH =9) AS bb 
ON aa.msc = bb.tower_dmnt
AND aa.msisdn = bb.msisdn
AND aa.subs_no = bb.subs_no
AND aa.day =bb.day;

2458383

CREATE TABLE sbx_falcon.Sachin_other_tower_dmnt_agg_Sep2020 as
SELECT msc,year,MONTH,DAY,
       round(sum(payload_mb),2) AS total_payload_mb_other_tower_dmnt,
       sum(EVENTS) AS total_events_other_tower_dmnt
FROM sbx_falcon.Sachin_other_tower_dmnt_Sep2020
GROUP BY msc,year,MONTH,DAY;
13670

CREATE TABLE sbx_falcon.Sachin_other_Overall_agg_Sep2020 as
SELECT msc,year,MONTH,DAY,
       round(sum(payload_mb),2) AS total_payload_mb_other_Overall,
       sum(EVENTS) AS total_events_other_Overall
FROM sbx_falcon.Sachin_other_Sep2020
GROUP BY msc,year,MONTH,DAY;

13750 

CREATE table Sachin_other_tower_dmnt_Overall as
SELECT aa.msc,aa.year,aa.MONTH,aa.DAY,aa.total_payload_mb_other_tower_dmnt,aa.total_events_other_tower_dmnt,
bb.total_payload_mb_other_Overall,bb.total_events_other_Overall
from sbx_falcon.Sachin_other_tower_dmnt_agg_Sep2020 as aa 
INNER JOIN (SELECT * from sbx_falcon.Sachin_other_Overall_agg_Sep2020) as bb
on aa.msc = bb.msc and aa.year = aa.year and aa.month= bb.month AND aa.day =bb.day;

13670



create table sbx_falcon.Sachin_stream_Sep2020 as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,SUM(total_events) as events,aa.year,
aa.month,aa.day 
from prd_bca.dpi_usg_h_stream_sdr as aa  inner join (SELECT * from sbx_falcon.Sachin_500_MSC_data) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 9 and tech ='4g'
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,aa.year,aa.month,aa.day;

4093177


CREATE TABLE sbx_falcon.Sachin_stream_tower_dmnt_Sep2020 AS
SELECT tech,msc,aa.subs_no,aa.msisdn,cgi,payload_mb,EVENTS,aa.year,aa.MONTH,aa.DAY
FROM sbx_falcon.Sachin_stream_Sep2020 AS aa
INNER JOIN (SELECT tower_dmnt,msisdn,subs_no,day FROM prd_bca.phoenix_mob_d WHERE year =2020 and MONTH =9) AS bb 
ON aa.msc = bb.tower_dmnt
AND aa.msisdn = bb.msisdn
AND aa.subs_no = bb.subs_no
AND aa.day =bb.day;

2021798 

CREATE TABLE sbx_falcon.Sachin_stream_tower_dmnt_agg_Sep2020 as
SELECT msc,year,MONTH,DAY,
       round(sum(payload_mb),2) AS total_payload_mb_stream_tower_dmnt,
       sum(EVENTS) AS total_events_stream_tower_dmnt
FROM sbx_falcon.Sachin_stream_tower_dmnt_Sep2020
GROUP BY msc,year,MONTH,DAY;
13655 

CREATE TABLE sbx_falcon.Sachin_stream_Overall_agg_Sep2020 as
SELECT msc,year,MONTH,DAY,
       round(sum(payload_mb),2) AS total_payload_mb_stream_Overall,
       sum(EVENTS) AS total_events_stream_Overall
FROM sbx_falcon.Sachin_stream_Sep2020
GROUP BY msc,year,MONTH,DAY;

13742

CREATE table Sachin_stream_tower_dmnt_Overall as
SELECT aa.msc,aa.year,aa.MONTH,aa.DAY,aa.total_payload_mb_stream_tower_dmnt,aa.total_events_stream_tower_dmnt,
bb.total_payload_mb_stream_Overall,bb.total_events_stream_Overall
from sbx_falcon.Sachin_stream_tower_dmnt_agg_Sep2020 as aa 
INNER JOIN (SELECT * from sbx_falcon.Sachin_stream_Overall_agg_Sep2020) as bb
on aa.msc = bb.msc and aa.year = aa.year and aa.month= bb.month AND aa.day =bb.day;

13655


create table sbx_falcon.Sachin_web_Sep2020 as
select bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,round(sum(aa.l4_throughput/(1024*1024)),2) as payload_mb,SUM(total_events) as events,aa.year,
aa.month,aa.day 
from prd_bca.dpi_usg_h_web_sdr as aa  inner join (SELECT * from sbx_falcon.Sachin_500_MSC_data) as bb 
on bb.cgi_dot = aa.cgi
where year = 2020 and month = 9 and tech ='4g'
group by bb.tech,bb.msc,aa.subs_no,aa.msisdn,aa.cgi,aa.year,aa.month,aa.day;

5012262


CREATE TABLE sbx_falcon.Sachin_web_tower_dmnt_Sep2020 AS
SELECT tech,msc,aa.subs_no,aa.msisdn,cgi,payload_mb,EVENTS,aa.year,aa.MONTH,aa.DAY
FROM sbx_falcon.Sachin_web_Sep2020 AS aa
INNER JOIN (SELECT tower_dmnt,msisdn,subs_no,day FROM prd_bca.phoenix_mob_d WHERE year =2020 and MONTH =9) AS bb 
ON aa.msc = bb.tower_dmnt
AND aa.msisdn = bb.msisdn
AND aa.subs_no = bb.subs_no
AND aa.day =bb.day;

2240282

CREATE TABLE sbx_falcon.Sachin_web_tower_dmnt_agg_Sep2020 as
SELECT msc,year,MONTH,DAY,
       round(sum(payload_mb),2) AS total_payload_mb_web_tower_dmnt,
       sum(EVENTS) AS total_events_web_tower_dmnt
FROM sbx_falcon.Sachin_web_tower_dmnt_Sep2020
GROUP BY msc,year,MONTH,DAY;
13666

CREATE TABLE sbx_falcon.Sachin_web_Overall_agg_Sep2020 as
SELECT msc,year,MONTH,DAY,
       round(sum(payload_mb),2) AS total_payload_mb_web_Overall,
       sum(EVENTS) AS total_events_web_Overall
FROM sbx_falcon.Sachin_web_Sep2020
GROUP BY msc,year,MONTH,DAY;
13746

CREATE table Sachin_web_tower_dmnt_Overall as
SELECT aa.msc,aa.year,aa.MONTH,aa.DAY,aa.total_payload_mb_web_tower_dmnt,aa.total_events_web_tower_dmnt,
bb.total_payload_mb_web_Overall,bb.total_events_web_Overall
from sbx_falcon.Sachin_web_tower_dmnt_agg_Sep2020 as aa 
INNER JOIN (SELECT * from sbx_falcon.Sachin_web_Overall_agg_Sep2020) as bb
on aa.msc = bb.msc and aa.year = aa.year and aa.month= bb.month AND aa.day =bb.day;

13666

CREATE table sbx_falcon.Sachin_4G_KPI_sep as
SELECT aa.msc,aa.year,aa.MONTH,aa.DAY,aa.total_payload_mb_IM_tower_dmnt,aa.total_events_IM_tower_dmnt,aa.total_payload_mb_IM_Overall,aa.total_events_IM_Overall,
bb.total_payload_mb_other_tower_dmnt,bb.total_events_other_tower_dmnt,bb.total_payload_mb_other_Overall,bb.total_events_other_Overall,
cc.total_payload_mb_stream_tower_dmnt,cc.total_events_stream_tower_dmnt,cc.total_payload_mb_stream_Overall,cc.total_events_stream_Overall,
dd.total_payload_mb_web_tower_dmnt,dd.total_events_web_tower_dmnt,dd.total_payload_mb_web_Overall,dd.total_events_web_Overall
from Sachin_IM_tower_dmnt_Overall as aa 
inner JOIN (SELECT * from  Sachin_other_tower_dmnt_Overall) as bb on aa.msc = bb.msc and aa.year = bb.year and aa.month= bb.month AND aa.day =bb.day
inner JOIN (SELECT * from  Sachin_stream_tower_dmnt_Overall) as cc on aa.msc = cc.msc and aa.year = cc.year and aa.month= cc.month AND aa.day =cc.day
inner JOIN (SELECT * from  Sachin_web_tower_dmnt_Overall) as dd on aa.msc = dd.msc and aa.year = dd.year and aa.month= dd.month AND aa.day =dd.day;
13649 


CREATE table sbx_falcon.sachin_pktloss_cell as
SELECT tower_id,year,month,day,
round(Sum(user_probe_ul_lost_pkt),2) as user_probe_ul_lost_pkt,
round(sum(user_probe_dw_lost_pkt),2) as user_probe_dw_lost_pkt,
round(sum(tcp_ul_packages_withpl),2) as tcp_ul_packages_withpl,
round(sum(tcp_dw_packages_withpl),2) as tcp_dw_packages_withpl
from prd_bca.nw_nqi_pktloss_cell_h
where year = 2020 and month =202009 
group by tower_id,year,month,day;
1026098 
11857413 

CREATE table sbx_falcon.sachin_pshttps_cell as
SELECT tower_id,year,month,day,
round(sum(count_session),2) as count_session,
round(sum(users),2) as users,
round(sum(air_port_duration),2) as air_port_duration,
round(avg(thrput_https_kbps),2) as thrput_https_kbps,
round(avg(avg_tcp_rtt),2) as avg_tcp_rtt
from prd_bca.nw_nqi_pshttps_cell_h
where year = 2020 and month =202009 and rat = 6
group by tower_id,year,month,day;
908357
4106956

CREATE table sbx_falcon.sachin_psweb_cell as
select tower_id,year,month,day,
round(sum(fst_page_req_num),2) as fst_page_req_num,
round(sum(fst_page_ack_num),2) as fst_page_ack_num,
round(sum(page_sr_delay_msel),2) as page_sr_delay_msel,
round(avg(page_succeed_times),2) as page_succeed_times,
round(sum(page_delay_msel),2) as page_delay_msel,
round(avg(page_avg_size),2) as page_avg_size
from prd_bca.nw_nqi_psweb_cell_h
where year = 2020 and month =202009
group by tower_id,year,month,day;

11243696
1024946
908317

CREATE table sbx_falcon.sachin_phoenix_mob_d as
(SELECT tower_dmnt,COUNT(DISTINCT msisdn) as tower_dmnt_total_user , sum(hwe_dmnt) as hwe_dmnt,year,month,day
from prd_bca.phoenix_mob_d
where year = 2020 and month = 9
group by tower_dmnt,year,month,day)
1032942 



CREATE table sbx_falcon.sachin_nw_nqi_KPI1 as 
SELECT aa.tower_id,aa.year,aa.month,aa.day,aa.user_probe_ul_lost_pkt,aa.user_probe_dw_lost_pkt,aa.tcp_ul_packages_withpl,aa.tcp_dw_packages_withpl,
bb.count_session,bb.users,bb.air_port_duration,bb.thrput_https_kbps,bb.avg_tcp_rtt,
cc.fst_page_req_num,cc.fst_page_ack_num,cc.page_sr_delay_msel,cc.page_succeed_times,cc.page_delay_msel,cc.page_avg_size,
dd.tower_dmnt_total_user,dd.hwe_dmnt
from sbx_falcon.sachin_pktloss_cell as aa
inner join (select * from sbx_falcon.sachin_pshttps_cell) as bb on aa.tower_id = bb.tower_id  and aa.year = bb.year and aa.month = bb.month and aa.day = bb.day
inner join (select * from sbx_falcon.sachin_psweb_cell) as cc on aa.tower_id = cc.tower_id and aa.year = cc.year and aa.month = cc.month and aa.day = cc.day
inner join (select * from sbx_falcon.sachin_phoenix_mob_d) as dd on aa.tower_id = dd.tower_dmnt and cast(SUBSTR(cast(aa.day as string),-2,2) as int)=dd.day
907545




create table sbx_falcon.sachin_nw_nqi_KPI1 as
select *, SUBSTR(cast(day as string),-2,2) as day1 from sbx_falcon.sachin_nw_nqi_KPI;

select *,tower_dmnt_total_user,hwe_dmnt
from sbx_falcon.sachin_nw_nqi_KPI1 as aa
inner join (select * from sbx_falcon.sachin_phoenix_mob_d) as bb on aa.tower_id = bb.tower_dmnt and aa.days=bb.day


SUBSTR(cast(day as string),-2,2)

CREATE table sbx_falcon.sachin_ran_4g_KPI_daily as
select cellname,year,month,day,round(SUM(ldlavguserthpmbpsa)/Sum(ldlavguserthpmbpsb),2) as ldlavguserthroughputmbps_counterSum,
round((SUM(lavgpdcpcellthpdlmbpsa)/sum(lavgpdcpcellthpdlmbpsb*1000)),2) as lavgpdcpcellthroughputdlmbps_counterSum,
round(SUM(ldllatencymsa)/sum(ldllatencymsb),2) as ldllatencyms_counterSum,
round(SUM(ldlrblockutilra)/sum(ldlrblockutilrb),2) as ldlresourceblockutilizingrate_counterSum, 
round(sum(lsinra)/sum(lsinrb),2) as lsinr_counterSum,
round(SUM(lcqin)/sum(lcqid),2) as lcqi_counterSum,
round(((sum(lqpskratea)/sum(lqpskrateb))*100),2) as lqpskrate_counterSum,
round(((sum(ltxrank2ratea)/sum(ltxrank2rateb))*100),2) as ltxrank2rate_counterSum,
round(((sum(ltxrank4ratea)/sum(ltxrank4rateb))*100),2) as ltxrank4rate_counterSum,
round(sum(ldltrafficvolumegb),2) as ldltrafficvolumegb_counterSum,
round(sum(ldlpacketlosspdcp),2) as ldlpacketlosspdcp_counterSum,
round(sum(lhosrinterratb),2) as lhosrinterratb_counterSum,
round(SUM(lavgsimultaneousrrcconnectedues),2) as lavgsimultaneousrrcconnectedues_counterSum
from prd_bca.nw_ran_4g_cell_h
where  year =2020 and month =202009
group by cellname,year,month,day
4984487 

CREATE table sbx_falcon.sachin_AViL as
select cellname,year,month,day,
round(SUM(((100-lnetworkelementavailability)*0.6)),2) as lnetworkelementavailability1,
round(SUM(((100-lnetworkelementavailabilityauto)*0.6)),2) as lnetworkelementavailabilityauto1
from prd_bca.nw_ran_4g_cell_h
where  year =2020 and month =202009
group by cellname,year,month,day
4984487

CREATE table sbx_falcon.sachin_AViL2 as
select aa.cellname,aa.year,aa.month,aa.day,lnetworkelementavailability1,lnetworkelementavailabilityauto1 from sbx_falcon.sachin_AViL
inner join (select DISTINCT * FROM amp.xqi_gcell_4g_w WHERE regional_10 in ('JABO 1') AND DAY = 20200919 
and msc in ('JAW-JK-KYB-1970','JAW-BT-TNG-1765','JAW-JK-KYB-1906','JAW-JK-CKG-0295','JAW-JK-TJP-2422') as bb
on aa.cellname=bb.cellname


CREATE table sbx_falcon.sachin_AViL2 as
select cellname,year,month,day,
round(((1440 - lnetworkelementavailability1)*0.06944),2) as lnetworkelementavailability_counterSum,
round(((1440 - lnetworkelementavailabilityauto1)*0.06944),2) as lnetworkelementavailabilityauto_counterSum
from sbx_falcon.sachin_AViL
where  year =2020 and month =202009
group by cellname,year,month,day
4984487

CREATE table sbx_falcon.sachin_ran_4g_KPI_daily1 as
select aa.cellname,aa.year,aa.month,aa.day,ldlavguserthroughputmbps_counterSum,lavgpdcpcellthroughputdlmbps_counterSum,ldllatencyms_counterSum,
ldlresourceblockutilizingrate_counterSum,lsinr_counterSum,lcqi_counterSum,lqpskrate_counterSum,ltxrank2rate_counterSum,
ltxrank4rate_counterSum,ldltrafficvolumegb_counterSum,ldlpacketlosspdcp_counterSum,lhosrinterratb_counterSum,lavgsimultaneousrrcconnectedues_counterSum
bb.lnetworkelementavailability_counterSum,bb.lnetworkelementavailabilityauto_counterSum
from sbx_falcon.sachin_ran_4g_KPI_daily as aa
inner join (select * from sbx_falcon.sachin_AViL2) as bb 
on aa.cellname = bb.cellname and aa.day = bb.day 
4984487

CREATE table sbx_falcon.sachin_ran_4g_KPI_tower1 as
select aa.cellname,aa.year,aa.month,aa.day,ldlavguserthroughputmbps_counterSum,lavgpdcpcellthroughputdlmbps_counterSum,ldllatencyms_counterSum,
ldlresourceblockutilizingrate_counterSum,lsinr_counterSum,lcqi_counterSum,lqpskrate_counterSum,ltxrank2rate_counterSum,
ltxrank4rate_counterSum,ldltrafficvolumegb_counterSum,ldlpacketlosspdcp_counterSum,lhosrinterratb_counterSum,lavgsimultaneousrrcconnectedues_counterSum,
lnetworkelementavailability_counterSum,lnetworkelementavailabilityauto_counterSum
from sbx_falcon.sachin_ran_4g_KPI_daily1 as aa
inner join (select * from sbx_falcon.Sachin_500_MSC_data2) as bb 
on aa.cellname = bb.cellname 

130879

CREATE table sbx_falcon.sachin_ran_4g_KPI_tower_agg as
select msc,year,month,day,
round(AVG(ldlavguserthroughputmbps_counterSum),2) as ldlavguserthroughputmbps_counterSum,
round(AVG(lavgpdcpcellthroughputdlmbps_counterSum),2) as lavgpdcpcellthroughputdlmbps_counterSum,
round(AVG(ldllatencyms_counterSum),2) as ldllatencyms_counterSum,
round(AVG(ldlresourceblockutilizingrate_counterSum),2) as ldlresourceblockutilizingrate_counterSum, 
round(AVG(lsinr_counterSum),2) as lsinr_counterSum,
round(AVG(lcqi_counterSum),2) as lcqi_counterSum,
round(AVG(lqpskrate_counterSum),2) as lqpskrate_counterSum,
round(avg(ltxrank2rate_counterSum),2) as ltxrank2rate_counterSum,
round(avg(ltxrank4rate_counterSum),2) as ltxrank4rate_counterSum,
round(sum(ldltrafficvolumegb_counterSum),2) as ldltrafficvolumegb_counterSum,
round(sum(ldlpacketlosspdcp_counterSum),2) as ldlpacketlosspdcp_counterSum,
round(sum(lhosrinterratb_counterSum),2) as lhosrinterratb_counterSum,
round(SUM(lavgsimultaneousrrcconnectedues_counterSum),2) as lavgsimultaneousrrcconnectedues_counterSum,
round(avg(lnetworkelementavailability_counterSum),2) as lnetworkelementavailability_counterSum,
round(avg(lnetworkelementavailabilityauto_counterSum),2) as lnetworkelementavailabilityauto_counterSum
from sbx_falcon.sachin_ran_4g_KPI_tower1
group by msc,year,month,day

13354

select DISTINCT * FROM amp.xqi_gcell_4g_w WHERE regional_10 in ('JABO 1') AND DAY = 20200919




CREATE table sbx_falcon.sachin_nw_nqi_KP as 
select bb.msc,aa.cellname,year,month,day,hour,ldlavguserthpmbpsa,ldlavguserthpmbpsb,lavgpdcpcellthpdlmbpsa,lavgpdcpcellthpdlmbpsb,lavgsimultaneousrrcconnectedues,ldllatencymsa,
ldllatencymsb,ldlrblockutilra,ldlrblockutilrb,lsinra,lsinrb,lcqin,lcqid,lqpskratea,lqpskrateb,ltxrank2ratea,ltxrank2rateb,ltxrank4ratea,ltxrank4rateb,
ldltrafficvolumegb,lnetworkelementavailability,lnetworkelementavailabilityauto,lhosrinterratb,ldlpacketlosspdcp
from prd_bca.nw_ran_4g_cell_h as aa
inner join (SELECT * from sbx_falcon.Sachin_500_MSC_data2) as bb on aa.cellname=bb.cellname
where  year =2020 and month =202009
3195005


CREATE table sbx_falcon.sachin_nw_nqi_busiest as
SELECT *, ROW_NUMBER() OVER(PARTITION BY msc,day ORDER BY lavgsimultaneousrrcconnectedues desc) AS RowNumberRank
from sbx_falcon.sachin_nw_nqi_KP
where lavgsimultaneousrrcconnectedues IS NOT NULL
3195005
3178222 

CREATE table sbx_falcon.sachin_nw_nqi_busiest_kpi as
select msc,year,month,day,HOUR as busiest_HOUR,
round((ldlavguserthpmbpsa)/(ldlavguserthpmbpsb),2) as ldlavguserthroughputmbps_busiest,
round(((lavgpdcpcellthpdlmbpsa)/(lavgpdcpcellthpdlmbpsb*1000)),2) as lavgpdcpcellthroughputdlmbps_busiest,
round((ldllatencymsa)/(ldllatencymsb),2) as ldllatencyms_busiest,
round((ldlrblockutilra)/(ldlrblockutilrb),2) as ldlresourceblockutilizingrate_busiest, 
round((lsinra)/(lsinrb),2) as lsinr_busiest,
round((lcqin)/(lcqid),2) as lcqi_busiest,
round((((lqpskratea)/(lqpskrateb))*100),2) as lqpskrate_busiest,
round((((ltxrank2ratea)/(ltxrank2rateb))*100),2) as ltxrank2rate_busiest,
round((((ltxrank4ratea)/(ltxrank4rateb))*100),2) as ltxrank4rate_busiest,
round((ldltrafficvolumegb),2) as ldltrafficvolumegb_busiest,
round((ldlpacketlosspdcp),2) as ldlpacketlosspdcp_busiest,
round((lhosrinterratb),2) as lhosrinterratb_busiest,
round((lavgsimultaneousrrcconnectedues),2) as lavgsimultaneousrrcconnectedues_busiest,
round((lnetworkelementavailability),2) as lnetworkelementavailability_busiest,
round((lnetworkelementavailabilityauto),2) as lnetworkelementavailabilityauto_busiest
from sbx_falcon.sachin_nw_nqi_busiest 
where RowNumberRank =1

13352




CREATE table sbx_falcon.sachin_nw_nqi_median as 
select bb.msc,aa.cellname,year,month,day,hour,ldlavguserthpmbpsa,ldlavguserthpmbpsb,lavgpdcpcellthpdlmbpsa,lavgpdcpcellthpdlmbpsb,lavgsimultaneousrrcconnectedues,ldllatencymsa,
ldllatencymsb,ldlrblockutilra,ldlrblockutilrb,lsinra,lsinrb,lcqin,lcqid,lqpskratea,lqpskrateb,ltxrank2ratea,ltxrank2rateb,ltxrank4ratea,ltxrank4rateb,
ldltrafficvolumegb,lnetworkelementavailability,lnetworkelementavailabilityauto,lhosrinterratb,ldlpacketlosspdcp
from prd_bca.nw_ran_4g_cell_h as aa
inner join (SELECT * from sbx_falcon.Sachin_500_MSC_data2) as bb on aa.cellname=bb.cellname
where  year =2020 and month =202009 and hour >=8 and hour <=22;
1962915 


CREATE table sbx_falcon.sachin_nw_nqi_median11 as
 SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY msc,day ORDER BY lavgsimultaneousrrcconnectedues) AS Rnk,
        COUNT(*) OVER (PARTITION BY msc,day) AS Cnt
    FROM sbx_falcon.sachin_nw_nqi_median
    ;
    
CREATE table sbx_falcon.sachin_nw_nqi_median12 as    
SELECT *
FROM sbx_falcon.sachin_nw_nqi_median11
WHERE Rnk  IN ((Cnt + 1)/2, (Cnt + 2)/2)
ORDER BY cellname,day;
13346

CREATE table sbx_falcon.sachin_nw_nqi_median_kpi1 as
select msc,year,month,day,HOUR as median_HOUR,
round((ldlavguserthpmbpsa)/(ldlavguserthpmbpsb),2) as ldlavguserthroughputmbps_median,
round(((lavgpdcpcellthpdlmbpsa)/(lavgpdcpcellthpdlmbpsb*1000)),2) as lavgpdcpcellthroughputdlmbps_median,
round((ldllatencymsa)/(ldllatencymsb),2) as ldllatencyms_median,
round((ldlrblockutilra)/(ldlrblockutilrb),2) as ldlresourceblockutilizingrate_median, 
round((lsinra)/(lsinrb),2) as lsinr_median,
round((lcqin)/(lcqid),2) as lcqi_median,
round((((lqpskratea)/(lqpskrateb))*100),2) as lqpskrate_median,
round((((ltxrank2ratea)/(ltxrank2rateb))*100),2) as ltxrank2rate_median,
round((((ltxrank4ratea)/(ltxrank4rateb))*100),2) as ltxrank4rate_median,
round((ldltrafficvolumegb),2) as ldltrafficvolumegb_median,
round((ldlpacketlosspdcp),2) as ldlpacketlosspdcp_median,
round((lhosrinterratb),2) as lhosrinterratb_median,
round((lavgsimultaneousrrcconnectedues),2) as lavgsimultaneousrrcconnectedues_median,
round((lnetworkelementavailability),2) as lnetworkelementavailability_median,
round((lnetworkelementavailabilityauto),2) as lnetworkelementavailabilityauto_median
from sbx_falcon.sachin_nw_nqi_median12 

13346
13343
9676
CREATE table sbx_falcon.sachin_ran_4g_KPI1 as
select aa.msc,aa.year,aa.month,aa.day,aa.ldlavguserthroughputmbps_counterSum,aa.lavgpdcpcellthroughputdlmbps_counterSum,aa.lavgsimultaneousrrcconnectedues_counterSum,aa.ldllatencyms_counterSum
,aa.ldlresourceblockutilizingrate_counterSum,aa.lsinr_counterSum,aa.lcqi_counterSum,aa.lqpskrate_counterSum,aa.ltxrank2rate_counterSum,aa.ltxrank4rate_counterSum
,aa.ldltrafficvolumegb_counterSum,aa.lnetworkelementavailability_counterSum,aa.lnetworkelementavailabilityauto_counterSum,aa.ldlpacketlosspdcp_counterSum
,bb.busiest_HOUR,bb.ldlavguserthroughputmbps_busiest,bb.lavgpdcpcellthroughputdlmbps_busiest,bb.lavgsimultaneousrrcconnectedues_busiest,bb.ldllatencyms_busiest,bb.ldlresourceblockutilizingrate_busiest
,bb.lsinr_busiest,bb.lcqi_busiest,bb.lqpskrate_busiest,bb.ltxrank2rate_busiest,bb.ltxrank4rate_busiest,bb.ldltrafficvolumegb_busiest,bb.lnetworkelementavailability_busiest,bb.lnetworkelementavailabilityauto_busiest,bb.ldlpacketlosspdcp_busiest
,cc.median_HOUR,cc.ldlavguserthroughputmbps_median,cc.lavgpdcpcellthroughputdlmbps_median,cc.lavgsimultaneousrrcconnectedues_median,cc.ldllatencyms_median,cc.ldlresourceblockutilizingrate_median
,cc.lsinr_median,cc.lcqi_median,cc.lqpskrate_median,cc.ltxrank2rate_median,cc.ltxrank4rate_median,cc.ldltrafficvolumegb_median,cc.lnetworkelementavailability_median,cc.lnetworkelementavailabilityauto_median,cc.ldlpacketlosspdcp_median
from sbx_falcon.sachin_ran_4g_KPI_tower_agg as aa
inner join (select * from sbx_falcon.sachin_nw_nqi_busiest_kpi) as bb on aa.msc = bb.msc and aa.year = bb.year and aa.month= bb.month AND aa.day =bb.day
inner JOIN (SELECT * from sbx_falcon.sachin_nw_nqi_median_kpi1) as cc on aa.msc = cc.msc and aa.year = cc.year and aa.month= cc.month AND aa.day =cc.day

13343
13344
CREATE table sbx_falcon.sachin_ran_4g_nqi_KPI1 as
select aa.msc,aa.year,aa.month,aa.day,
bb.user_probe_ul_lost_pkt,bb.user_probe_dw_lost_pkt,bb.tcp_ul_packages_withpl,bb.tcp_dw_packages_withpl,
bb.count_session,bb.users,bb.air_port_duration,bb.thrput_https_kbps,bb.avg_tcp_rtt,
bb.fst_page_req_num,bb.fst_page_ack_num,bb.page_sr_delay_msel,bb.page_succeed_times,bb.page_delay_msel,bb.page_avg_size,
bb.tower_dmnt_total_user,bb.hwe_dmnt,
aa.ldlavguserthroughputmbps_counterSum,aa.lavgpdcpcellthroughputdlmbps_counterSum,aa.lavgsimultaneousrrcconnectedues_counterSum,aa.ldllatencyms_counterSum
,aa.ldlresourceblockutilizingrate_counterSum,aa.lsinr_counterSum,aa.lcqi_counterSum,aa.lqpskrate_counterSum,aa.ltxrank2rate_counterSum,aa.ltxrank4rate_counterSum
,aa.ldltrafficvolumegb_counterSum,aa.lnetworkelementavailability_counterSum,aa.lnetworkelementavailabilityauto_counterSum,aa.ldlpacketlosspdcp_counterSum
,aa.busiest_HOUR,aa.ldlavguserthroughputmbps_busiest,aa.lavgpdcpcellthroughputdlmbps_busiest,aa.lavgsimultaneousrrcconnectedues_busiest,aa.ldllatencyms_busiest,aa.ldlresourceblockutilizingrate_busiest
,aa.lsinr_busiest,aa.lcqi_busiest,aa.lqpskrate_busiest,aa.ltxrank2rate_busiest,aa.ltxrank4rate_busiest,aa.ldltrafficvolumegb_busiest,aa.lnetworkelementavailability_busiest,aa.lnetworkelementavailabilityauto_busiest,aa.ldlpacketlosspdcp_busiest
,aa.median_HOUR,aa.ldlavguserthroughputmbps_median,aa.lavgpdcpcellthroughputdlmbps_median,aa.lavgsimultaneousrrcconnectedues_median,aa.ldllatencyms_median,aa.ldlresourceblockutilizingrate_median
,aa.lsinr_median,aa.lcqi_median,aa.lqpskrate_median,aa.ltxrank2rate_median,aa.ltxrank4rate_median,aa.ldltrafficvolumegb_median,aa.lnetworkelementavailability_median,aa.lnetworkelementavailabilityauto_median,aa.ldlpacketlosspdcp_median
from sbx_falcon.sachin_ran_4g_KPI1 as aa
inner join (select * from sbx_falcon.sachin_nw_nqi_KPI1) as bb on aa.msc = bb.tower_id and aa.year = bb.year and aa.month= bb.month AND aa.day =bb.day;
12582
12582

CREATE table sbx_falcon.Sachin_4G_KPI_sep_2020_1 as
SELECT aa.msc,bb.year,bb.MONTH,bb.DAY,aa.total_payload_mb_IM_tower_dmnt,aa.total_events_IM_tower_dmnt,aa.total_payload_mb_IM_Overall,aa.total_events_IM_Overall,
aa.total_payload_mb_other_tower_dmnt,aa.total_events_other_tower_dmnt,aa.total_payload_mb_other_Overall,aa.total_events_other_Overall,
aa.total_payload_mb_stream_tower_dmnt,aa.total_events_stream_tower_dmnt,aa.total_payload_mb_stream_Overall,aa.total_events_stream_Overall,
aa.total_payload_mb_web_tower_dmnt,aa.total_events_web_tower_dmnt,aa.total_payload_mb_web_Overall,aa.total_events_web_Overall,
bb.user_probe_ul_lost_pkt,bb.user_probe_dw_lost_pkt,bb.tcp_ul_packages_withpl,bb.tcp_dw_packages_withpl,
bb.count_session,bb.users,bb.air_port_duration,bb.thrput_https_kbps,bb.avg_tcp_rtt,
bb.fst_page_req_num,bb.fst_page_ack_num,bb.page_sr_delay_msel,bb.page_succeed_times,bb.page_delay_msel,bb.page_avg_size,
bb.tower_dmnt_total_user,bb.hwe_dmnt,
bb.ldlavguserthroughputmbps_counterSum,bb.lavgpdcpcellthroughputdlmbps_counterSum,bb.lavgsimultaneousrrcconnectedues_counterSum,bb.ldllatencyms_counterSum
,bb.ldlresourceblockutilizingrate_counterSum,bb.lsinr_counterSum,bb.lcqi_counterSum,bb.lqpskrate_counterSum,bb.ltxrank2rate_counterSum,bb.ltxrank4rate_counterSum
,bb.ldltrafficvolumegb_counterSum,bb.lnetworkelementavailability_counterSum,bb.lnetworkelementavailabilityauto_counterSum,bb.ldlpacketlosspdcp_counterSum
,bb.busiest_HOUR,bb.ldlavguserthroughputmbps_busiest,bb.lavgpdcpcellthroughputdlmbps_busiest,bb.lavgsimultaneousrrcconnectedues_busiest,bb.ldllatencyms_busiest,bb.ldlresourceblockutilizingrate_busiest
,bb.lsinr_busiest,bb.lcqi_busiest,bb.lqpskrate_busiest,bb.ltxrank2rate_busiest,bb.ltxrank4rate_busiest,bb.ldltrafficvolumegb_busiest,bb.lnetworkelementavailability_busiest,bb.lnetworkelementavailabilityauto_busiest,bb.ldlpacketlosspdcp_busiest
,bb.median_HOUR,bb.ldlavguserthroughputmbps_median,bb.lavgpdcpcellthroughputdlmbps_median,bb.lavgsimultaneousrrcconnectedues_median,bb.ldllatencyms_median,bb.ldlresourceblockutilizingrate_median
,bb.lsinr_median,bb.lcqi_median,bb.lqpskrate_median,bb.ltxrank2rate_median,bb.ltxrank4rate_median,bb.ldltrafficvolumegb_median,bb.lnetworkelementavailability_median,bb.lnetworkelementavailabilityauto_median,bb.ldlpacketlosspdcp_median
from sbx_falcon.Sachin_4G_KPI_sep as aa 
inner JOIN (SELECT DISTINCT *  from  sbx_falcon.sachin_ran_4g_nqi_KPI1) as bb on aa.msc = bb.msc and  aa.day=cast(SUBSTR(cast(bb.day as string),-2,2) as int)

12452
12452
