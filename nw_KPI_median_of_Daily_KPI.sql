
create table sbx_falcon.Sachin_Site_jabo1_jabo2_3 as
select distinct msc,cellname,dlearfcn,dlbandwidth
from amp.xqi_gcell_4g_w where day =20200919 and regional_10 in ('JABO 1','JABO 2')


##Add Tower specific config related features like BW & N/w KPIs (DL Thp, Latency, Packetloss, prb util, rrc_Users)

Pick median of Daily KPI across Sept'20'
Jabo1 + Jabo2



CREATE table sbx_falcon.sachin_ran_4g_KPI_daily1 as
select cellname,year,month,day,round(SUM(ldlavguserthpmbpsa)/Sum(ldlavguserthpmbpsb),2) as ldlavguserthroughputmbps_counterSum,
round((SUM(lavgpdcpcellthpdlmbpsa)/sum(lavgpdcpcellthpdlmbpsb*1000)),2) as lavgpdcpcellthroughputdlmbps_counterSum,
round(SUM(ldllatencymsa)/sum(ldllatencymsb),2) as ldllatencyms_counterSum,
round(SUM(ldlrblockutilra)/sum(ldlrblockutilrb),2) as ldlresourceblockutilizingrate_counterSum, 
round(sum(ldlpacketlosspdcp),2) as ldlpacketlosspdcp_counterSum,
round(SUM(lavgsimultaneousrrcconnectedues),2) as lavgsimultaneousrrcconnectedues_counterSum
from prd_bca.nw_ran_4g_cell_h
where  year =2020 and month =202009
group by cellname,year,month,day
4984487

CREATE table sbx_falcon.sachin_ran_4g_KPI_tower_agg2 as
select msc,year,month,day,
round(avg((ldlavguserthroughputmbps_counterSum)),2) as DL_Thp_User_level,
round(avg(lavgpdcpcellthroughputdlmbps_counterSum),2) as DL_Thp_Cell_level,
round(AVG(ldllatencyms_counterSum),2) as latency,
round(AVG(ldlresourceblockutilizingrate_counterSum),2) as Prb_utilization, 
round(SUM(lavgsimultaneousrrcconnectedues_counterSum),2) as rrc_Users
from sbx_falcon.sachin_ran_4g_KPI_tower1
group by msc,year,month,day;






CREATE table sbx_falcon.sachin_nw_nqi_lowest as
SELECT *, ROW_NUMBER() OVER(PARTITION BY msc ORDER BY rrc_Users Asc) AS RowNumberRank
from sbx_falcon.sachin_ran_4g_KPI_tower_agg2
;

CREATE table sbx_falcon.sachin_nw_nqi_lowest1 as
select * from sbx_falcon.sachin_nw_nqi_lowest
where RowNumberRank >1;


CREATE table sbx_falcon.sachin_nw_KPI_1 as
 SELECT *,ROW_NUMBER() OVER (PARTITION BY msc ORDER BY rrc_Users) AS Rnk,
 COUNT(*) OVER (PARTITION BY msc) AS Cnt
 FROM sbx_falcon.sachin_nw_nqi_lowest1
    ;
    
CREATE table sbx_falcon.sachin_nw_KPI_2 as    
SELECT *
FROM sbx_falcon.sachin_nw_KPI_1
WHERE Rnk  IN ((Cnt + 1)/2, (Cnt + 2)/2)
ORDER BY msc,day;


select * from sbx_falcon.sachin_nw_KPI_2;


create table sbx_falcon.Sachin_Site_jabo1_jabo2_bw1 as
select distinct msc,sector,dlearfcn,dlbandwidth
from amp.xqi_gcell_4g_w where day =20200919 and regional_10 in ('JABO 1','JABO 2');


create table sbx_falcon.Sachin_Site_jabo1_jabo2_bw3 as
select msc,sum(dlbandwidth) as bandwidth from sbx_falcon.Sachin_Site_jabo1_jabo2_bw1
group by msc;


create table sbx_falcon.sachin_nw_KPI_median_of_Daily_KPI as 
select a1.msc,bandwidth,year,month,day,rrc_Users,DL_Thp_User_level,DL_Thp_cell_level,latency,Prb_utilization
from sbx_falcon.sachin_nw_KPI_2 as a1
inner join (select msc,bandwidth from sbx_falcon.Sachin_Site_jabo1_jabo2_bw3) as b1 on a1.msc=b1.msc;


CREATE table sbx_falcon.sachin_nw_nqi_median3 as 
select bb.msc,aa.cellname,year,month,day,hour,ldlavguserthpmbpsa,ldlavguserthpmbpsb,lavgpdcpcellthpdlmbpsa,lavgpdcpcellthpdlmbpsb,lavgsimultaneousrrcconnectedues,ldllatencymsa,
ldllatencymsb,ldlrblockutilra,ldlrblockutilrb,ldlpacketlosspdcp
from prd_bca.nw_ran_4g_cell_h as aa
inner join (SELECT distinct * from sbx_falcon.Sachin_Site_jabo1_jabo2_3) as bb on aa.cellname=bb.cellname
where  year =2020 and month =202009 and hour >=8 and hour <=22;


CREATE table sbx_falcon.sachin_nw_nqi_tower_agg as 
select bb.msc,year,month,day,hour,
sum(ldlavguserthpmbpsa) as ldlavguserthpmbpsa,
sum(ldlavguserthpmbpsb) as ldlavguserthpmbpsb,
sum(lavgpdcpcellthpdlmbpsa) as lavgpdcpcellthpdlmbpsa,
sum(lavgpdcpcellthpdlmbpsb) as lavgpdcpcellthpdlmbpsb,
sum(lavgsimultaneousrrcconnectedues) as lavgsimultaneousrrcconnectedues,
sum(ldllatencymsa) as ldllatencymsa,
sum(ldllatencymsb) as ldllatencymsb,
sum(ldlrblockutilra) as ldlrblockutilra,
sum(ldlrblockutilrb) as ldlrblockutilrb,
sum(ldlplosspdcpa) as ldlplosspdcpa,
sum(ldlplosspdcpb) as ldlplosspdcpb
from prd_bca.nw_ran_4g_cell_h as aa
inner join (SELECT distinct * from sbx_falcon.Sachin_Site_jabo1_jabo2_3) as bb on aa.cellname=bb.cellname
where  year =2020 and month =202009 and hour >=8 and hour <=22
group by bb.msc,year,month,day,hour;

CREATE table sbx_falcon.sachin_nw_nqi_tower_agg_median as
 SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY msc,day ORDER BY lavgsimultaneousrrcconnectedues) AS Rnk,
        COUNT(*) OVER (PARTITION BY msc,day) AS Cnt
    FROM sbx_falcon.sachin_nw_nqi_tower_agg
    ;
    
CREATE table sbx_falcon.sachin_nw_nqi_tower_agg_median1  as    
SELECT *
FROM sbx_falcon.sachin_nw_nqi_tower_agg_median
WHERE Rnk  IN ((Cnt + 1)/2, (Cnt + 2)/2)
ORDER BY cellname,day;



CREATE table sbx_falcon.sachin_nw_nqi_median_kpi3 as
select msc,year,month,day,HOUR as median_HOUR
round((ldlavguserthpmbpsa)/(ldlavguserthpmbpsb),2) as DL_Thp_User_level_MoM,
round(((lavgpdcpcellthpdlmbpsa)/(lavgpdcpcellthpdlmbpsb*1000)),2) as DL_Thp_cell_level_MoM,
round((ldllatencymsa)/(ldllatencymsb),2) as latency_MOM,
round(((ldlrblockutilra)/(ldlrblockutilrb))*100,2) as Prb_utilization_MOM, 
round((lavgsimultaneousrrcconnectedues),2) as rrc_Users_MOM,
round(((ldlplosspdcpa)/(ldlplosspdcpb))*100,2) as DL_packet_loss_MOM, 
from sbx_falcon.sachin_nw_nqi_tower_agg_median1



CREATE table sbx_falcon.sachin_nw_nqi_lowest1 as
SELECT *, ROW_NUMBER() OVER(PARTITION BY msc ORDER BY rrc_Users Asc) AS RowNumberRank
from sbx_falcon.sachin_nw_nqi_median_kpi14
;

CREATE table sbx_falcon.sachin_nw_nqi_lowest11 as
select * from sbx_falcon.sachin_nw_nqi_lowest1
where RowNumberRank >1;


CREATE table sbx_falcon.sachin_nw_KPI_4 as
 SELECT *,ROW_NUMBER() OVER (PARTITION BY msc ORDER BY rrc_Users) AS Rnk,
 COUNT(*) OVER (PARTITION BY msc) AS Cnt
 FROM sachin_nw_nqi_median_kpi3
    ;
    
CREATE table sbx_falcon.sachin_nw_KPI5 as    
SELECT *
FROM sbx_falcon.sachin_nw_KPI4
WHERE Rnk  IN ((Cnt + 1)/2, (Cnt + 2)/2)
ORDER BY msc,day;

create table sbx_falcon.sachin_nw_KPI_median_of_Daily_KPI_2 as 
select a1.msc,bandwidth,b1.year,b1.month,b1.day,rrc_Users_MOM,DL_Thp_User_level_MOM,DL_Thp_cell_level_MOM,latency_MOM,Prb_utilization_MOM
from sbx_falcon.sachin_nw_KPI_median_of_Daily_KPI as a1
inner join (select distinct * from sbx_falcon.sachin_nw_KPI5 ) as b1 on a1.msc=b1.msc;