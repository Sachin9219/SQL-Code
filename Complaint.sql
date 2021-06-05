
SELECT * FROM prd_xldf_itl_tb. f_d_cs_complaint where  m_month = 'oct';

CREATE table sbx_falcon.sachin_camplaint1 as 
SELECT *, CONCAT(reason_1, '-', reason_2, '-',reason_3) AS reason_123 from prd_xldf_itl_tb. f_d_cs_complaint
where m_month in ('September','october');

select * from  sbx_falcon.sachin_camplaint1 
where m_month in ('September','October');

create table sbx_falcon.sachin_camplaint1 as
select *, split_part(d_day,'-',1) as year1,split_part(d_day,'-',2) as month1 from sbx_falcon.sachin_camplaint
where m_month in ('September','October') ;
3349801


create table sbx_falcon.sachin_camplaint2 as
select msisdn,M_month,year,reason_1,reason_2,reason_3,reason_123 from sbx_falcon.sachin_camplaint1
where year = 2020
and reason_1 = 'Complaint'
and reason_2 in ('Coverage','Data Service','Mass Maintenance','Mass Problem','Mass Suspect','Network','Voice')
and reason_3 in ('4G Blank Signal','4G Unstable Signal','Blank Signal','Unstable Signal','4G Conn Browse Specific Web Failed','4G Connected But Browse to All Failed'
,'4G Slow Access To Internet','Connect To Internet Failed','Connected But Browse Specific Web Failed','Connected But Browse to All Failed','Disconnected'
,'Slow Access To Internet','Network Others','OC Drop')
;

270908

create table sbx_falcon.sachin_camplaint3 as
select msisdn,M_month,year,reason_123,
CASE WHEN reason_1='Complaint' THEN 1 else 0 END AS Complaint,
CASE WHEN reason_2='Coverage' THEN 1 else 0 END AS Coverage,
CASE WHEN reason_2='Data Service' THEN 1 else 0 END AS Data_Service,
CASE WHEN reason_2='Mass Maintenance' THEN 1 else 0 END AS Mass_Maintenance,
CASE WHEN reason_2='Mass Problem' THEN 1 else 0 END AS Mass_Problem,
CASE WHEN reason_2='Mass Suspect' THEN 1 else 0 END AS Mass_Suspect,
CASE WHEN reason_2='Network' THEN 1 else 0 END AS Network,
CASE WHEN reason_2='Voice' THEN 1 else 0 END AS Voice,
CASE WHEN reason_3='4G Blank Signal' THEN 1 else 0 END AS 4G_Blank_Signal,
CASE WHEN reason_3='4G Unstable Signal' THEN 1 else 0 END AS 4G_Unstable_Signal,
CASE WHEN reason_3='Blank Signal' THEN 1 else 0 END AS Blank_Signal,
CASE WHEN reason_3='Unstable Signal' THEN 1 else 0 END AS Unstable_Signal,
CASE WHEN reason_3='4G Conn Browse Specific Web Failed' THEN 1 else 0 END AS 4G_Conn_Browse_Specific_Web_Failed,
CASE WHEN reason_3='4G Connected But Browse to All Failed' THEN 1 else 0 END AS 4G_Connected_But_Browse_to_All_Failed,
CASE WHEN reason_3='Connect To Internet Failed' THEN 1 else 0 END AS Connect_To_Internet_Failed,
CASE WHEN reason_3='Connected But Browse Specific Web Failed' THEN 1 else 0 END AS Connected_But_Browse_Specific_Web_Failed,
CASE WHEN reason_3='Connected But Browse to All Failed' THEN 1 else 0 END AS Connected_But_Browse_to_All_Failed,
CASE WHEN reason_3='Disconnected' THEN 1 else 0 END AS Disconnected,
CASE WHEN reason_3='Slow Access To Internet' THEN 1 else 0 END AS Slow_Access_To_Internet,
CASE WHEN reason_3='Network Others' THEN 1 else 0 END AS Network_Others,
CASE WHEN reason_3='4G Slow Access To Internet' THEN 1 else 0 END AS 4G_Slow_Access_To_Internet,
CASE WHEN reason_3='OC Drop' THEN 1 else 0 END AS OC_Drop

from sbx_falcon.sachin_camplaint2 ;
270908


create table sbx_falcon.sachin_camplaint4 as
select msisdn,M_month,year,
SUM(Complaint) as Complaint,
sum(Coverage) as Coverage,
sum(Data_Service) as Data_Service,
sum(Mass_Maintenance) as Mass_Maintenance,
sum(Mass_Problem) as Mass_Problem,
sum(Mass_Suspect)as Mass_Suspect,
sum(Network) as Network,
sum(Voice) as Voice,
sum(4G_Blank_Signal) as 4G_Blank_Signal,
sum(4G_Unstable_Signal) as 4G_Unstable_Signal,
sum(Blank_Signal) as Blank_Signal,
sum(Unstable_Signal) as Unstable_Signal,
sum(4G_Conn_Browse_Specific_Web_Failed) as 4G_Conn_Browse_Specific_Web_Failed),
sum(4G_Connected_But_Browse_to_All_Failed) as 4G_Connected_But_Browse_to_All_Failed,
sum(Connect_To_Internet_Failed) as Connect_To_Internet_Failed,
sum(Connected_But_Browse_Specific_Web_Failed) as Connected_But_Browse_Specific_Web_Failed,
sum(Connected_But_Browse_to_All_Failed) as Connected_But_Browse_to_All_Failed,
sum(Disconnected) as Disconnected,
sum(Slow_Access_To_Internet) as Slow_Access_To_Internet,
sum(Network_Others) as Network_Others,
sum(4G_Slow_Access_To_Internet) as 4G_Slow_Access_To_Internet,
sum(OC_Drop) as OC_Drop
from sbx_falcon.sachin_camplaint3
group by msisdn,M_month,year
117450
97823
distint user 


create table sbx_falcon.sachin_camplaint5 as
select tower_dmnt_dom_all,aa.msisdn,m_month,year,complaint,coverage,data_service,mass_maintenance,mass_problem,mass_suspect
,network,voice,4g_blank_signal,4g_unstable_signal,blank_signal,unstable_signal,4g_conn_browse_specific_web_failed
,4g_connected_but_browse_to_all_failed,connect_to_internet_failed,connected_but_browse_specific_web_failed,connected_but_browse_to_all_failed
,disconnected,slow_access_to_internet,network_others,4g_slow_access_to_internet,oc_drop
from sbx_falcon.sachin_camplaint4  as aa
inner join (select distinct msisdn,tower_dmnt_dom_all from prd_bca.phoenix_mob_m where m_month in (202009,202010)) as bb
on aa.msisdn=cast(bb.msisdn as string);
108464

create table sbx_falcon.sachin_revenue_loss as 
SELECT distinct msc,tower_id from amp.xqi_gcell_4g_w

 where tower_id in ('1001','1003','1011','1015','1018','1023','1029','1031','1041','1054','1067','1079','1079','1090',
 '1110','1110','1122','1126','1130','1140','1144','1154','1163','1180','1184','1186','1194','1196','1196',
 '1202','1208','1208','1208','1213','1213','1214','1218','1224','1229','1232','1234','1240','1249','1255','1256',
 '1260','1267','1277','1281','1306','1315','1322','1327','1337','1337','1348','1353','1365','1373','1397','1399',
 '1400','1417','1420','1421','1428','1428','1437','1437','1459','185','190','191','195','195','2412242','2412440',
 '2412480L1','2412480L1','2412480L1','2413076','2413090','2413417','2413421','2413424','2413444','2413449','2413450',
 '2413464','2413466','2413479','2413485','2413545','2413548','2414028','2414071','2414083','2414083','2414115','2414140',
 '2414217','2414219','2414231','2414234','2414244','2414252','2414292','2414407','2414445','2415223','2415224','2415404',
 '2415408','2415428','2415848','2415854','2416058','2416200','2417142','2417146','2417146','2417254','2417282','2417362',
 '2417362','2417388','2417412OL1','2417506','2417506','2417575','2417973','241MC2271','241MC3923','2423601','2433050L1',
 '2433704L1','2443351L1','2464124L1','31','3103','3103','3106','3106','3130R','3153','3180','3183','3183','3185','3187',
 '3194','3227','3237','3254','3412276G','3422074G','3422079G','3424370G','3424398G','3431525G','3431842G','3441048G',
 '3441060G','3441062G','3441597G','3443390G','3443390G','3443454G','3443475G','3443848G','3453998G','3462137G','3462137G',
 '3462144G','3462588G','3462772G','3462781G','3462781G','3464363G','3464419G','3464419G','3464745G','3471113G','3471787G',
 '3471859G','3472823G','3472849G','3472986G','3472996G','3473056G','400','400','41','417','451','451','457','457','4904',
 '4941','4946','4956','4960','4991','4991','4999','5796G','5796G','580','583','587','587','653','658','677','681','927','980',
 '980','983','985','A053','A062','A094','A163','A164','A165','A238','A243','A367','A367','A464','A486','MC0729','MC2412231',
 'MC2412431','MC2412602','MC2412646','MC2412649','MC2412649','MC2412830','MC2412860','MC2413298','MC2413417','MC2413850','MC2413899',
 'MC2413939','MC2413939','MC2413981','MC2414062','MC2414078','MC2414215','MC2414215','MC2414278','MC2414290','MC2414448','MC2414449',
 'MC2414608','MC2414625','MC2414625','MC2414630','MC2414839','MC2414841','MC2414853','MC2415040','MC2415058','MC2415838','MC2415853',
 'MC2415861','MC2415887','MC3412772G','MC3412793G','MC3412793G','MC3412831G','MC3443105G','MC3464668G','MC3464782G','MC3464830G','MC3472854G',
 'MC3472927G','MC3472967G','MCA484','SH2413081','SH2413081','SH2413081','SH3412932G','SH3424467G','SH3431777G','SH3433643G','SH3443786G');
 
290



create table sbx_falcon.sachin_camplaint_oct as
select * from sbx_falcon.sachin_camplaint5
where M_month = 'October'
51306

create table sbx_falcon.sachin_camplaint_sep as
select * from sbx_falcon.sachin_camplaint5
where M_month = 'September'
57158

create table sbx_falcon.sachin_revenue_loss_analysis as 
select tower_dmnt_dom_all,msc,aa.msisdn,m_month,year,complaint,coverage,data_service,mass_maintenance,mass_problem,mass_suspect
,network,voice,4g_blank_signal,4g_unstable_signal,blank_signal,unstable_signal,4g_conn_browse_specific_web_failed
,4g_connected_but_browse_to_all_failed,connect_to_internet_failed,connected_but_browse_specific_web_failed,connected_but_browse_to_all_failed
,disconnected,slow_access_to_internet,network_others,4g_slow_access_to_internet,oc_drop
from sbx_falcon.sachin_camplaint_oct as aa
inner join (select msc from sbx_falcon.sachin_revenue_loss) as bb
on aa.tower_dmnt_dom_all=bb.msc;
51306

create table sbx_falcon.sachin_revenue_loss_analysis as 
select tower_dmnt_dom_all,msc,aa.msisdn,m_month,year,complaint,coverage,data_service,mass_maintenance,mass_problem,mass_suspect
,network,voice,4g_blank_signal,4g_unstable_signal,blank_signal,unstable_signal,4g_conn_browse_specific_web_failed
,4g_connected_but_browse_to_all_failed,connect_to_internet_failed,connected_but_browse_specific_web_failed,connected_but_browse_to_all_failed
,disconnected,slow_access_to_internet,network_others,4g_slow_access_to_internet,oc_drop
from sbx_falcon.sachin_camplaint_oct as aa
inner join (select msc from sbx_falcon.sachin_revenue_loss) as bb
on aa.tower_dmnt_dom_all=bb.msc;
total complaint:139837
complaint on revenue loss tower:5202

create table sbx_falcon.sachin_LTE_leakage
(
msc varchar(500)
)
insert into sbx_falcon.sachin_LTE_leakage(msc)
values ('JAW-JK-TJP-2518','JAW-JK-GGP-0673','JAW-BT-TNG-0311','JAW-JK-GGP-0777','JAW-JK-TNA-2840','JAW-JK-TJP-2414','JAW-BT-TNG-1772','JAW-JK-TJP-2112','JAW-JK-TNA-2562','JAW-JK-KYB-1906','JAW-JK-TJP-2428','JAW-JK-GGP-1060','JAW-JK-GGP-0651','JAW-JK-GGP-0903','JAW-JK-GGP-2617','JAW-JK-GGP-1047','JAW-JK-GGP-1051','JAW-JK-TJP-0028','JAW-JK-TJP-2241','JAW-BT-TNG-1681','JAW-JK-CKG-0448','JAW-JK-TNA-2628','JAW-BT-TNG-1663','JAW-JK-GGP-0859','JAW-BT-TNG-1698','JAW-JK-TNA-3031','JAW-JK-TJP-2120','JAW-JK-KYB-2046','JAW-JK-TJP-2279','JAW-JK-TJP-2422','JAW-JK-TJP-2184','JAW-JK-TJP-2107','JAW-JK-GGP-1071','JAW-JK-TJP-0941','JAW-BT-TNG-1451','JAW-JK-GGP-1045','JAW-JK-TJP-2495','JAW-JK-TJP-2146','JAW-JK-GGP-1055','JAW-BT-TNG-1765','JAW-BT-TNG-1735','JAW-JB-DPK-1838','JAW-JK-GGP-0995','JAW-JB-DPK-1788','JAW-JK-CKG-0182','JAW-BT-CPT-0388','JAW-JK-TNA-3059','JAW-JK-GGP-0934','JAW-JB-DPK-1641','JAW-JK-TJP-2176','JAW-JK-TJP-2103','JAW-JB-DPK-1584','JAW-JK-TJP-2509','JAW-JK-TJP-2172','JAW-JK-CKG-0450','JAW-JK-GGP-0806','JAW-BT-TNG-1603','JAW-BT-CPT-0325','JAW-JB-DPK-1670','JAW-JK-TJP-2449','JAW-JK-KSU-1124','JAW-JK-GGP-0830','JAW-JK-TNA-3013','JAW-JK-GGP-1008','JAW-JK-CKG-0195','JAW-JK-TNA-3113','JAW-BT-CPT-0276','JAW-JB-DPK-1782','JAW-JB-DPK-1649','JAW-JK-TJP-2443','JAW-JK-KYB-1446','JAW-JK-CKG-0177','JAW-JK-GGP-0671','JAW-JK-KYB-2988','JAW-BT-TNG-1808','JAW-JB-DPK-1563','JAW-JK-TJP-2533','JAW-JK-GGP-0936','JAW-BT-CPT-0298','JAW-JK-GGP-0681','JAW-JK-KYB-1922','JAW-JK-CKG-0498','JAW-JB-DPK-1783','JAW-JK-GGP-0720','JAW-JB-DPK-1575','JAW-JK-GGP-0576','JAW-JB-DPK-1725','JAW-JK-GGP-1000','JAW-JK-GGP-0922','JAW-BT-TNG-1899','JAW-JK-GGP-1024','JAW-JK-TJP-2387','JAW-BT-CPT-0420','JAW-JK-KSU-0368','JAW-JB-DPK-1562','JAW-JK-TJP-2092','JAW-JK-TJP-2178','JAW-JK-TJP-2133','JAW-JK-GGP-1079','JAW-BT-CPT-0329','JAW-BT-TNG-1721','JAW-JB-DPK-1624','JAW-JK-GGP-0989','JAW-JB-DPK-1852','JAW-JK-CKG-0058','JAW-JK-CKG-0395','JAW-JK-CKG-0164','JAW-JB-DPK-1845','JAW-JK-KYB-1970','JAW-BT-CPT-0400','JAW-BT-CPT-0410','JAW-JK-GGP-0947','JAW-BT-CPT-0278','JAW-JK-TJP-2300','JAW-JB-DPK-1859','JAW-JB-DPK-1843','JAW-BT-TNG-1840','JAW-BT-TNG-1639','JAW-JK-TJP-2318','JAW-JK-TJP-2151','JAW-JB-DPK-1865','JAW-JK-TJP-2140','JAW-JB-DPK-1658','JAW-JK-GGP-0821','JAW-JK-GGP-0814','JAW-JK-GGP-0907','JAW-JK-GGP-0667','JAW-JK-GGP-0919','JAW-JB-DPK-1735','JAW-JB-DPK-1679','JAW-JB-DPK-1642','JAW-BT-CPT-0350','JAW-JK-GGP-0904','JAW-JB-DPK-1654','JAW-JK-GGP-0648','JAW-BT-TNG-1597','JAW-BT-CPT-0320','JAW-JK-TJP-2481','JAW-JK-TJP-2289','JAW-JK-TJP-2253','JAW-JK-GGP-0766','JAW-JK-CKG-0566','JAW-JB-DPK-0420','JAW-BT-TNG-1781','JAW-JK-TJP-2490','JAW-JK-CKG-0560','JAW-BT-CPT-0379','JAW-JK-GGP-2086','JAW-JK-TNA-2884','JAW-JK-TNA-3058','JAW-JK-TJP-2226','JAW-BT-CPT-0131','JAW-BT-TGR-2007','JAW-JK-TJP-2312','JAW-JK-KSU-1125','JAW-JB-DPK-1634','JAW-JK-TJP-2427','JAW-BT-TNG-1674','JAW-JK-GGP-1070','JAW-BT-CPT-0397','JAW-JB-DPK-1612','JAW-JK-TJP-2388','JAW-JK-CKG-0295','JAW-JK-GGP-0728','JAW-JK-CKG-0097','JAW-JB-DPK-1698','JAW-JK-TJP-2231','JAW-BT-CPT-0371','JAW-JK-CKG-0493','JAW-BT-TNG-1816','JAW-JK-GGP-0809','JAW-JK-KYB-1900','JAW-JK-TJP-2106','JAW-BT-TNG-1580','JAW-BT-CPT-1850','JAW-BT-CPT-0348','JAW-JB-DPK-1555','JAW-JB-DPK-1820','JAW-BT-CPT-0402','JAW-JK-GGP-1092','JAW-JK-TJP-2323','JAW-JK-CKG-0183','JAW-JK-GGP-0655','JAW-BT-TNG-1905','JAW-JB-DPK-0531','JAW-JK-KYB-1186','JAW-JK-TJP-2295','JAW-JK-GGP-0805','JAW-JK-KYB-1998','JAW-JK-TJP-2476','JAW-JK-GGP-1026','JAW-JK-KYB-2050','JAW-JK-GGP-0899','JAW-JB-DPK-1853','JAW-JK-TJP-2520','JAW-JK-TNA-3029','JAW-JK-GGP-0764','JAW-JB-DPK-1797','JAW-JK-GGP-0597','JAW-JK-GGP-0771','JAW-BT-TNG-1722','JAW-JK-TJP-2292','JAW-JK-GGP-0918','JAW-JB-DPK-1671','JAW-JK-CKG-0357','JAW-JK-GGP-0879','JAW-JK-TNA-2865','JAW-BT-CPT-0225','JAW-JK-GGP-0916','JAW-JK-GGP-0755','JAW-JK-TJP-2145','JAW-BT-TNG-1916','JAW-JK-TJP-2123','JAW-BT-TNG-1801','JAW-JK-GGP-1134','JAW-JK-TJP-2530','JAW-JK-GGP-1073','JAW-JK-GGP-1112','JAW-BT-CPT-0373','JAW-JB-DPK-0445','JAW-BT-TNG-1848','JAW-BT-TNG-1921','JAW-JK-TJP-2494','JAW-JK-CKG-0485','JAW-JK-TJP-2223','JAW-JK-KSU-1126','JAW-JK-KYB-1663','JAW-JB-DPK-1822','JAW-JK-GGP-2079','JAW-BT-TNG-1659','JAW-JK-KYB-1286','JAW-JK-TJP-2493','JAW-JK-CKG-0043','JAW-JK-KYB-1153','JAW-JB-CBI-0012','JAW-JK-TNA-0409','JAW-JK-TJP-2484','JAW-JK-TJP-2062','JAW-JK-CKG-0459','JAW-JB-DPK-1586','JAW-JB-DPK-1587','JAW-BT-CPT-0412','JAW-JK-KYB-1422','JAW-JB-DPK-2480','JAW-JB-DPK-1656','JAW-JK-TJP-0029','JAW-JK-GGP-1121','JAW-JK-GGP-0635','JAW-JK-CKG-3067','JAW-JK-TJP-2516','JAW-JK-CKG-0534','JAW-JK-GGP-0590','JAW-JB-DPK-1048','JAW-JK-TJP-2508','JAW-JB-DPK-1631','JAW-JK-CKG-0377','JAW-BT-TNG-1605','JAW-BT-CPT-0396','JAW-JK-GGP-0732','JAW-BT-CPT-1528','JAW-JK-TJP-2084','JAW-BT-CPT-0270')


create table sbx_falcon.sachin_LTE_leakage_analysis as 
select ,aa.msc, bb.msc as mscLTE_leakage, aa.msisdn,m_month,year,complaint,coverage,data_service,mass_maintenance,mass_problem,mass_suspect
,network,voice,4g_blank_signal,4g_unstable_signal,blank_signal,unstable_signal,4g_conn_browse_specific_web_failed
,4g_connected_but_browse_to_all_failed,connect_to_internet_failed,connected_but_browse_specific_web_failed,connected_but_browse_to_all_failed
,disconnected,slow_access_to_internet,network_others,4g_slow_access_to_internet,oc_drop,
from sbx_falcon.sachin_camplaint_sep as aa
inner join (select msc from sbx_falcon.sachin_LTE_leakage) as bb
on aa.msc=bb.msc;
total complaint:139837
complaint on revenue loss tower:5202


9980
157845

create table sbx_falcon.sachin_LTE_leakage_analysis1 as 
select tower_dmnt_dom_all, bb.msc as msc_LTE_leakage,bb.leakage_category,aa.msisdn,m_month,year,complaint,coverage,data_service,mass_maintenance,mass_problem,mass_suspect
,network,voice,4g_blank_signal,4g_unstable_signal,blank_signal,unstable_signal,4g_conn_browse_specific_web_failed
,4g_connected_but_browse_to_all_failed,connect_to_internet_failed,connected_but_browse_specific_web_failed,connected_but_browse_to_all_failed
,disconnected,slow_access_to_internet,network_others,4g_slow_access_to_internet,oc_drop
from sbx_falcon.sachin_camplaint_sep as aa
left join (select msc,leakage_category from sbx_falcon.LTE_Leakage_towers_Data_sep2020) as bb
on aa.tower_dmnt_dom_all=bb.msc;

1141

CREATE table sbx_falcon.sachin_LTE_leakage_analysis2 as 
Select * from sbx_falcon.sachin_LTE_leakage_analysis1 as a
INNER JOIN (SELECT  DISTINCT msc,regional_10 from amp.xqi_gcell_4g_w WHERE day =20200919 ) as b 
on a.tower_dmnt_dom_all = b.msc




