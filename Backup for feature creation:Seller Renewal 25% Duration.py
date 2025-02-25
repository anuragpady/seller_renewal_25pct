# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Database (One Time Run)

# COMMAND ----------

# %sql
# create database if not exists data_science_metastore.seller_renewal_25pct_featureStore;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Seller Base for Feature Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data
# MAGIC AS
# MAGIC SELECT
# MAGIC   sr.city,
# MAGIC   sr.Rank,
# MAGIC   sr.SELLER_UUID,
# MAGIC   p.user_id,
# MAGIC   p.email,
# MAGIC   ppn.number as phone_number,
# MAGIC   p.city_bounding_box_uuids,
# MAGIC   sr.SELLER_NAME,
# MAGIC   sr.PROFILE_TYPE,
# MAGIC   sr.OPPORTUNITY_ID,
# MAGIC   sr.SLOT_IDS,
# MAGIC   sr.PRODUCT_IDS,
# MAGIC   DATE(sr.MIN_START_DATETIME) as package_start_date,
# MAGIC   DATE(sr.MAX_END_DATETIME) as package_end_date,
# MAGIC   floor(datediff(DATE(sr.MAX_END_DATETIME), DATE(MIN_START_DATETIME))/4) as package_duration_oneFourth,
# MAGIC   date_add(DATE(sr.MIN_START_DATETIME), cast(floor(datediff(DATE(sr.MAX_END_DATETIME), DATE(sr.MIN_START_DATETIME))/4) as int)) as oneFourth_cutOff_date,
# MAGIC   datediff(DATE(sr.MAX_END_DATETIME), DATE(sr.MIN_START_DATETIME)) as package_duration,
# MAGIC   sr.OPPORTUNITY_TYPE,
# MAGIC   sr.RAM_ID,
# MAGIC   sr.account_owner_id,
# MAGIC   YEAR(sr.MAX_END_DATETIME) as renewal_due_year,
# MAGIC   MONTH(sr.MAX_END_DATETIME) as renewal_due_month,
# MAGIC   SUM(opm.rate_price) as rate_price,
# MAGIC   SUM(opm.sales_price) as sales_price,
# MAGIC   nrr.IS_ADDON 
# MAGIC FROM
# MAGIC   edw_product_catalog.product_derived.seller_retention_extended_renewal_window sr
# MAGIC   INNER JOIN edw_product_catalog.product_derived.new_renewal_report nrr on sr.opportunity_id = nrr.opportunity_id 
# MAGIC   INNER JOIN housing_clients_production.profiles p ON sr.SELLER_UUID = p.profile_uuid
# MAGIC   INNER JOIN housing_clients_production.profiles_phone_numbers ppn ON p.id = ppn.profile_id
# MAGIC   LEFT JOIN sapna.opportunity_package_mappings_v2 opm ON opm.opportunity_id = sr.opportunity_id
# MAGIC
# MAGIC WHERE DATE(MAX_END_DATETIME) >= date(current_date())
# MAGIC   AND  date_add(DATE(sr.MIN_START_DATETIME), cast(floor(datediff(DATE(sr.MAX_END_DATETIME), DATE(sr.MIN_START_DATETIME))/4) as int)) > date(current_date())-7
# MAGIC   AND  date_add(DATE(sr.MIN_START_DATETIME), cast(floor(datediff(DATE(sr.MAX_END_DATETIME), DATE(sr.MIN_START_DATETIME))/4) as int)) <= date(current_date())
# MAGIC   AND  date_add(DATE(sr.MIN_START_DATETIME), cast(floor(datediff(DATE(sr.MAX_END_DATETIME), DATE(sr.MIN_START_DATETIME))/4) as int)) <= date(current_date())
# MAGIC   AND LOWER(sr.profile_type) IN ('broker','builder')
# MAGIC   and lower(nrr.Renewal_Window) not in ('add on')
# MAGIC   AND datediff(DATE(sr.MAX_END_DATETIME), DATE(sr.MIN_START_DATETIME)) >= 90
# MAGIC GROUP BY sr.city, sr.Rank, sr.SELLER_UUID, p.user_id, p.email, ppn.number, p.city_bounding_box_uuids, sr.SELLER_NAME, sr.PROFILE_TYPE, sr.OPPORTUNITY_ID, sr.SLOT_IDS, sr.PRODUCT_IDS, DATE(sr.MIN_START_DATETIME), DATE(sr.MAX_END_DATETIME), sr.OPPORTUNITY_TYPE, sr.RAM_ID, sr.account_owner_id, YEAR(sr.MAX_END_DATETIME), MONTH(sr.MAX_END_DATETIME),nrr.is_addon
# MAGIC ORDER BY sr.SELLER_UUID, sr.Rank ASC;
# MAGIC
# MAGIC SELECT * FROM data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct oneFourth_cutOff_date,count(distinct OPPORTUNITY_ID) as cnt
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data 
# MAGIC GROUP BY oneFourth_cutOff_date

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Leads Base data for City level calculations

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data
# MAGIC
# MAGIC with leads_base as 
# MAGIC
# MAGIC (select lsu.profile_uuid,
# MAGIC lsu.lead_id,
# MAGIC lm.service_object_id as listing_id,
# MAGIC
# MAGIC date(lm.created_at)  as lead_date,
# MAGIC lm.lead_generator,
# MAGIC lm.lead_filler,
# MAGIC lm.lead_type,
# MAGIC lm.traffic_sourcemedium,
# MAGIC lsu.profile_uuid as seller_uuid
# MAGIC from housing_leads_production.lead_supply_users lsu 
# MAGIC left join housing_leads_production.lead_masters lm on lsu.lead_id = lm.id
# MAGIC where lsu.created_at >= date(current_date())-1130 and lsu.created_at <= date(current_date())
# MAGIC and lm.created_at >= date(current_date())-1130 and lm.created_at <= date(current_date())
# MAGIC group by all
# MAGIC order by seller_uuid,lead_date desc),
# MAGIC
# MAGIC properties_details_base as 
# MAGIC (select f.id as listing_id,
# MAGIC ft_bounding.name as city,
# MAGIC case when at2.display_name in ('1 RK','1 BHK') then '1RK/1BHK'
# MAGIC      when at2.display_name in ('2 BHK','3 BHK','3+ BHK') then display_name else 'Others' end as bhk_type,
# MAGIC case when pt.name in ('Independent House','Villa') then 'Independent House/Villa'
# MAGIC      when pt.name in ('Apartment') then 'Apartment'
# MAGIC      when pt.name in ('Independent Floor') then 'Independent Floor' else 'Others' end as property_type,
# MAGIC max(case when f.is_buy = True then cast(f.price as int) else 
# MAGIC              cast(get_json_object(uf.user_flat_details, '$.rent') as int) end) as listing_price
# MAGIC from housing_production.flats f
# MAGIC left join housing_production.user_flats uf on f.id = uf.flat_id
# MAGIC left join housing_production.feature_types ft_bounding on ft_bounding.id  = f.region_bounding_box_id
# MAGIC left join housing_production.property_types pt on pt.id = f.property_type_id 
# MAGIC left join housing_production.apartment_types at2 on at2.id = f.apartment_type_id 
# MAGIC where f.id in (select distinct listing_id from leads_base)
# MAGIC group by 1,2,3,4),
# MAGIC
# MAGIC property_leads_combined as 
# MAGIC (select pdb.city,lb.seller_uuid,
# MAGIC lb.listing_id,
# MAGIC pdb.listing_price,
# MAGIC pdb.property_type,
# MAGIC pdb.bhk_type,
# MAGIC lb.lead_id,
# MAGIC lb.lead_type,
# MAGIC lb.lead_date,
# MAGIC lb.lead_filler,
# MAGIC lb.lead_generator,
# MAGIC lb.traffic_sourcemedium
# MAGIC from leads_base lb 
# MAGIC left join properties_details_base pdb on lb.listing_id = pdb.listing_id
# MAGIC group by all
# MAGIC order by 1,2 desc),
# MAGIC
# MAGIC imputation_calculation as 
# MAGIC (select seller_uuid,
# MAGIC median(plc.listing_price) as mean_listing_price,
# MAGIC mode(plc.property_type) as property_type
# MAGIC from property_leads_combined as plc 
# MAGIC group by 1)
# MAGIC
# MAGIC select plc.city,plc.seller_uuid,
# MAGIC plc.listing_id,
# MAGIC case when plc.listing_price is null then ic.mean_listing_price else plc.listing_price end as listing_price,
# MAGIC plc.property_type,
# MAGIC plc.bhk_type,
# MAGIC plc.lead_id,
# MAGIC plc.lead_date,
# MAGIC plc.lead_filler,
# MAGIC plc.lead_generator,
# MAGIC case when lower(lead_generator) like "%churn%" or lower(lead_generator) like "%shadow%" then 'churn/shadow'
# MAGIC      when lead_generator in ('ImageContactRequest','ContactRequest','Request_callback') then 'CR/ImageCR/ReCallback'
# MAGIC      when lower(lead_generator) = 'crm' then 'crm'
# MAGIC      when lower(lead_generator) in ('facebook','FB_Marketplace') then 'facebook/FB_Marketplace' else 'others' end as lead_generator_new,
# MAGIC
# MAGIC plc.traffic_sourcemedium
# MAGIC from property_leads_combined plc
# MAGIC left join imputation_calculation ic on plc.seller_uuid = ic.seller_uuid
# MAGIC where plc.city is not null
# MAGIC and plc.listing_id is not null
# MAGIC group by all
# MAGIC order by seller_uuid,listing_id;
# MAGIC
# MAGIC
# MAGIC select * from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data order by city limit 4 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Available Slot in a Package

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE data_science_metastore.seller_renewal_25pct_featureStore.seller_slots_package
# MAGIC select seller_uuid,opportunity_id,package_start_date,package_end_date,
# MAGIC count(distinct exploded_slot_ids) as count_slots_pkg
# MAGIC from 
# MAGIC
# MAGIC (select sr.OPPORTUNITY_ID,sr.seller_uuid,
# MAGIC  DATE(MIN_START_DATETIME) as package_start_date,
# MAGIC   DATE(MAX_END_DATETIME) as package_end_date,
# MAGIC  EXPLODE(SPLIT(REGEXP_REPLACE(ARRAY_JOIN(sr.slot_ids, ','), '[\\[\\]]', ''), ',')) AS exploded_slot_ids 
# MAGIC from edw_product_catalog.product_derived.seller_retention_extended_renewal_window  sr 
# MAGIC where sr.seller_uuid is not null
# MAGIC and sr.opportunity_id is not null
# MAGIC group by all) z
# MAGIC group by 1,2,3,4
# MAGIC order by 1,2,3;
# MAGIC
# MAGIC select * from data_science_metastore.seller_renewal_25pct_featureStore.seller_slots_package limit 5
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Previous Packages History

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists data_science_metastore.seller_renewal_25pct_featureStore.seller_previous_packages

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE data_science_metastore.seller_renewal_25pct_featureStore.seller_previous_packages
# MAGIC
# MAGIC
# MAGIC with seller_package_base as (
# MAGIC select city,SELLER_UUID,
# MAGIC OPPORTUNITY_ID,
# MAGIC package_start_date,
# MAGIC package_end_date
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data
# MAGIC group by all
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC previous_package as (
# MAGIC   select sr.seller_uuid,sr.opportunity_id,
# MAGIC   date(sr.MIN_START_DATETIME) as previous_package_start_date,
# MAGIC   date(sr.max_end_datetime) as previous_package_end_date,
# MAGIC   case when nrr.RENEWAL_WINDOW in ('45D Renewed') then 1 else 0 end as Renewal_Flag
# MAGIC   FROM
# MAGIC   edw_product_catalog.product_derived.seller_retention_extended_renewal_window sr
# MAGIC   INNER JOIN edw_product_catalog.product_derived.new_renewal_report nrr on sr.opportunity_id = nrr.opportunity_id 
# MAGIC   where lower(nrr.Renewal_Window) not in ('add on')
# MAGIC   group by all
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC prefinal as (
# MAGIC select b.city,b.seller_uuid,b.opportunity_id,
# MAGIC b.package_start_date,
# MAGIC b.package_end_date,
# MAGIC pp.opportunity_id as previous_opportunity_id,
# MAGIC pp.previous_package_start_date,
# MAGIC pp.previous_package_end_date,
# MAGIC pp.Renewal_Flag as Renewal_Flag_previous,
# MAGIC rank() over (partition by b.SELLER_UUID,b.OPPORTUNITY_ID,b.package_start_date order by pp.previous_package_start_date desc) as rk
# MAGIC from seller_package_base b 
# MAGIC left join previous_package pp on pp.seller_uuid = b.seller_uuid 
# MAGIC             and pp.previous_package_start_date < date_sub(b.package_end_date,30)
# MAGIC             and pp.previous_package_end_date >= date_sub(b.package_start_date,1500)     
# MAGIC order by seller_uuid,package_start_date desc,pp.previous_package_start_date desc )
# MAGIC
# MAGIC select city,
# MAGIC seller_uuid,
# MAGIC opportunity_id,
# MAGIC package_start_date,
# MAGIC package_end_date,
# MAGIC coalesce(sum(case when  previous_opportunity_id is not null then 1 else 0 end),0) as seller_tot_packages_previous,
# MAGIC coalesce(sum(Renewal_Flag_previous),0) as seller_tot_renewal_previous
# MAGIC
# MAGIC from prefinal
# MAGIC where city is not null
# MAGIC group by 1,2,3,4,5
# MAGIC order by city,seller_uuid,opportunity_id,package_start_date desc;
# MAGIC
# MAGIC select * from data_science_metastore.seller_renewal_25pct_featureStore.seller_previous_packages limit 4
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leads and Listings related features

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_listings_features_prefinal
# MAGIC
# MAGIC with seller_base as (
# MAGIC select city,seller_uuid,a.opportunity_id,package_start_date,package_end_date,oneFourth_cutOff_date,
# MAGIC package_duration,
# MAGIC rate_price,sales_price
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data a 
# MAGIC ),
# MAGIC
# MAGIC distribution_property_type_25pctDuration as (
# MAGIC
# MAGIC                 select city,seller_uuid,OPPORTUNITY_ID,
# MAGIC                 sum(case when property_type = 'Independent House/Villa' then cnt_listings else 0 end) as listings_IndependentHouseVilla_25pctDuration,
# MAGIC                 sum(case when property_type = 'Apartment' then cnt_listings else 0 end) as listings_Apartment_25pctDuration,
# MAGIC                 sum(case when property_type = 'Independent Floor' then cnt_listings else 0 end) as listings_Independent_Floor_25pctDuration,
# MAGIC                 sum(case when property_type = 'Others' then cnt_listings else 0 end) as listings_Others_25pctDuration
# MAGIC                 from
# MAGIC                 (select a.city,a.seller_uuid,a.OPPORTUNITY_ID,property_type,count(distinct listing_id) as cnt_listings
# MAGIC                 from seller_base a
# MAGIC                 left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data b on a.seller_uuid = b.seller_uuid 
# MAGIC                                                 and b.lead_date>= a.package_start_date and b.lead_date <= a.oneFourth_cutOff_date
# MAGIC                 group by 1,2,3,4) x
# MAGIC                 group by 1,2,3),
# MAGIC
# MAGIC distribution_bhk_type_25pctDuration as (
# MAGIC
# MAGIC                 select city,seller_uuid,OPPORTUNITY_ID,
# MAGIC                 sum(case when bhk_type = '1RK/1BHK' then cnt_listings else 0 end) as listings_1RK1BHK_25pctDuration,
# MAGIC                 sum(case when bhk_type = '2 BHK' then cnt_listings else 0 end) as listings_2BHK_25pctDuration,
# MAGIC                 sum(case when bhk_type = '3 BHK' then cnt_listings else 0 end) as listings_3BHK_25pctDuration,
# MAGIC                 sum(case when bhk_type = '3+ BHK' then cnt_listings else 0 end) as listings_3plusBHK_25pctDuration,
# MAGIC                 sum(case when bhk_type = 'Others' then cnt_listings else 0 end) as listings_Others_25pctDuration
# MAGIC                 from
# MAGIC                 (select a.city,a.seller_uuid,a.OPPORTUNITY_ID,bhk_type,count(distinct listing_id) as cnt_listings
# MAGIC                 from seller_base a
# MAGIC                 left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data b on a.seller_uuid = b.seller_uuid 
# MAGIC                                                 and b.lead_date>= a.package_start_date and b.lead_date <= a.oneFourth_cutOff_date
# MAGIC                 group by 1,2,3,4) x
# MAGIC                 group by 1,2,3),
# MAGIC
# MAGIC leads_distribution_property_type_25pctDuration as (
# MAGIC                 select a.city,a.seller_uuid,a.OPPORTUNITY_ID,
# MAGIC                 sum(case when property_type = 'Independent House/Villa' then 1 else 0 end) as leads_IndependentHouseVilla_25pctDuration,
# MAGIC                 sum(case when property_type = 'Apartment' then 1 else 0 end) as leads_Apartment_25pctDuration,
# MAGIC                 sum(case when property_type = 'Independent Floor' then 1 else 0 end) as leads_Independent_Floor_25pctDuration,
# MAGIC                 sum(case when property_type = 'Others' then 1 else 0 end) as leads_other_property_type_25pctDuration
# MAGIC                 from
# MAGIC                (select a.city,a.seller_uuid,a.OPPORTUNITY_ID,listing_id,property_type,lead_generator_new,lead_id
# MAGIC                 from seller_base a
# MAGIC                 left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data b on a.seller_uuid = b.seller_uuid 
# MAGIC                                                 and b.lead_date>= a.package_start_date and b.lead_date <= a.oneFourth_cutOff_date
# MAGIC                 group by 1,2,3,4,5,6,7) a
# MAGIC                 group by 1,2,3),
# MAGIC                 
# MAGIC leads_distribution_bhk_type_25pctDuration as (
# MAGIC                 select a.city,a.seller_uuid,a.OPPORTUNITY_ID,
# MAGIC                 sum(case when bhk_type = '1RK/1BHK' then 1 else 0 end) as leads_1RK1BHK_25pctDuration,
# MAGIC                 sum(case when bhk_type = '2 BHK' then 1 else 0 end) as leads_2BHK_25pctDuration,
# MAGIC                 sum(case when bhk_type = '3 BHK' then 1 else 0 end) as leads_3BHK_25pctDuration,
# MAGIC                 sum(case when bhk_type = '3+ BHK' then 1 else 0 end) as leads_3plusBHK_25pctDuration,
# MAGIC                 sum(case when bhk_type = 'Others' then 1 else 0 end) as leads_Others_25pctDuration
# MAGIC                 from
# MAGIC                 (select a.city,a.seller_uuid,a.OPPORTUNITY_ID,listing_id,bhk_type,lead_generator_new,lead_id
# MAGIC                 from seller_base a
# MAGIC                 left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data b on a.seller_uuid = b.seller_uuid and b.lead_date>= a.package_start_date 
# MAGIC                                             and b.lead_date <= a.oneFourth_cutOff_date
# MAGIC                 group by 1,2,3,4,5,6,7) a
# MAGIC                 group by 1,2,3),
# MAGIC
# MAGIC channel_leads_distribution_25pctDuration as (
# MAGIC                 select a.city,a.seller_uuid,a.OPPORTUNITY_ID,
# MAGIC                 sum(case when lead_generator_new = 'churn/shadow' then 1 else 0 end) as churn_shadow_leads_25pctDuration,
# MAGIC                 sum(case when lead_generator_new = 'CR/ImageCR/ReCallback' then 1 else 0 end) as CR_ImageCR_ReCallback_leads_25pctDuration,
# MAGIC                 sum(case when lead_generator_new = 'call/ivr' then 1 else 0 end) as call_ivr_leads_25pctDuration,
# MAGIC                 sum(case when lead_generator_new = 'crm' then 1 else 0 end) as crm_leads_25pctDuration,
# MAGIC                 sum(case when lead_generator_new = 'facebook/FB_Marketplace' then 1 else 0 end) as fb_fbM_leads_25pctDuration,
# MAGIC                 sum(case when lead_generator_new = 'others' then 1 else 0 end) as other_channel_leads_25pctDuration
# MAGIC                 from 
# MAGIC                 (select a.city,a.seller_uuid,a.OPPORTUNITY_ID,listing_id,property_type,lead_generator_new,lead_id
# MAGIC                 from seller_base a
# MAGIC                 left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data b on a.seller_uuid = b.seller_uuid 
# MAGIC                                                 and b.lead_date>= a.package_start_date and b.lead_date <= a.oneFourth_cutOff_date
# MAGIC                 
# MAGIC                 group by 1,2,3,4,5,6,7) a
# MAGIC                 group by 1,2,3 ),
# MAGIC
# MAGIC
# MAGIC leads_listings_25pctDuration as (
# MAGIC               select city,seller_uuid,OPPORTUNITY_ID,
# MAGIC               count(lead_id) as total_leads_25pctDuration,
# MAGIC               count(distinct listing_id) as tot_listings_25pctDuration
# MAGIC      
# MAGIC               from 
# MAGIC                     (
# MAGIC                     select a.city,a.seller_uuid,a.OPPORTUNITY_ID,
# MAGIC                     b.listing_id,
# MAGIC                     b.listing_price,
# MAGIC                     b.property_type,
# MAGIC                     b.lead_id,
# MAGIC                     b.lead_generator_new
# MAGIC                     from seller_base a
# MAGIC                     left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data b on a.seller_uuid = b.seller_uuid 
# MAGIC                                 and b.lead_date>= a.package_start_date and b.lead_date <= a.oneFourth_cutOff_date
# MAGIC                     group by all
# MAGIC                     order by 1,2,3 ) p
# MAGIC               group by 1,2,3
# MAGIC               order by 1,2),
# MAGIC
# MAGIC listings_price_25pctDuration as (
# MAGIC               select city,seller_uuid,OPPORTUNITY_ID,
# MAGIC               sum(listing_price) as tot_listing_price_25pctDuration
# MAGIC               from 
# MAGIC                     (
# MAGIC                     select a.city,a.seller_uuid,a.OPPORTUNITY_ID,
# MAGIC                     b.listing_id,
# MAGIC                     b.listing_price,
# MAGIC                     b.property_type
# MAGIC                     from seller_base a
# MAGIC                     left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data b on a.seller_uuid = b.seller_uuid 
# MAGIC                                 and b.lead_date>= a.package_start_date and b.lead_date <= a.oneFourth_cutOff_date
# MAGIC                     group by all
# MAGIC                     order by 1,2,3 ) p
# MAGIC               group by 1,2,3
# MAGIC               order by 1,2)
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC select a.city,a.seller_uuid,a.opportunity_id,
# MAGIC total_leads_25pctDuration,
# MAGIC tot_listings_25pctDuration,
# MAGIC tot_listing_price_25pctDuration,
# MAGIC
# MAGIC listings_IndependentHouseVilla_25pctDuration,
# MAGIC listings_Apartment_25pctDuration,
# MAGIC listings_Independent_Floor_25pctDuration,
# MAGIC n.listings_Others_25pctDuration as listings_OtherPropertyType_25pctDuration ,
# MAGIC leads_IndependentHouseVilla_25pctDuration,
# MAGIC leads_Apartment_25pctDuration,
# MAGIC leads_Independent_Floor_25pctDuration,
# MAGIC leads_other_property_type_25pctDuration,
# MAGIC
# MAGIC listings_1RK1BHK_25pctDuration,
# MAGIC listings_2BHK_25pctDuration,
# MAGIC listings_3BHK_25pctDuration,
# MAGIC listings_3plusBHK_25pctDuration,
# MAGIC p.listings_Others_25pctDuration as listings_OtherBHKType_25pctDuration,
# MAGIC leads_1RK1BHK_25pctDuration,
# MAGIC leads_2BHK_25pctDuration,
# MAGIC leads_3BHK_25pctDuration,
# MAGIC leads_3plusBHK_25pctDuration,
# MAGIC leads_Others_25pctDuration as leads_OtherBHKType_25pctDuration,
# MAGIC
# MAGIC churn_shadow_leads_25pctDuration,
# MAGIC CR_ImageCR_ReCallback_leads_25pctDuration,
# MAGIC call_ivr_leads_25pctDuration,
# MAGIC crm_leads_25pctDuration,
# MAGIC fb_fbM_leads_25pctDuration,
# MAGIC other_channel_leads_25pctDuration
# MAGIC
# MAGIC from seller_base a
# MAGIC
# MAGIC left join leads_listings_25pctDuration l on a.seller_uuid = l.seller_uuid and a.opportunity_id = l.OPPORTUNITY_ID
# MAGIC
# MAGIC left join listings_price_25pctDuration m on a.seller_uuid = m.seller_uuid and a.opportunity_id = m.OPPORTUNITY_ID
# MAGIC
# MAGIC left join distribution_property_type_25pctDuration n on a.seller_uuid = n.seller_uuid 
# MAGIC                                                               and a.opportunity_id = n.OPPORTUNITY_ID
# MAGIC left join leads_distribution_property_type_25pctDuration o on a.seller_uuid = o.seller_uuid 
# MAGIC                                                                and a.opportunity_id = o.OPPORTUNITY_ID
# MAGIC
# MAGIC left join distribution_bhk_type_25pctDuration p on a.seller_uuid = p.seller_uuid and a.opportunity_id = p.OPPORTUNITY_ID
# MAGIC
# MAGIC
# MAGIC left join leads_distribution_bhk_type_25pctDuration q on a.seller_uuid = q.seller_uuid 
# MAGIC                                                                and a.opportunity_id = q.OPPORTUNITY_ID
# MAGIC
# MAGIC left join channel_leads_distribution_25pctDuration r on a.seller_uuid = r.seller_uuid 
# MAGIC                                                                and a.opportunity_id = r.OPPORTUNITY_ID
# MAGIC
# MAGIC group by all
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC select * from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_listings_features_prefinal limit 4

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_listings_features_final
# MAGIC
# MAGIC select city,seller_uuid,opportunity_id,
# MAGIC
# MAGIC coalesce(total_leads_25pctDuration,0) as total_leads_25pctDuration,
# MAGIC coalesce(tot_listings_25pctDuration,0) as tot_listings_25pctDuration,
# MAGIC coalesce(tot_listing_price_25pctDuration,0) as tot_listing_price_25pctDuration,
# MAGIC
# MAGIC coalesce(listings_IndependentHouseVilla_25pctDuration,0) as listings_IndependentHouseVilla_25pctDuration,
# MAGIC coalesce(listings_Apartment_25pctDuration,0) as listings_Apartment_25pctDuration,
# MAGIC coalesce(listings_Independent_Floor_25pctDuration,0) as listings_Independent_Floor_25pctDuration,
# MAGIC coalesce(listings_OtherPropertyType_25pctDuration,0) as listings_OtherPropertyType_25pctDuration ,
# MAGIC coalesce(leads_IndependentHouseVilla_25pctDuration,0) as leads_IndependentHouseVilla_25pctDuration,
# MAGIC coalesce(leads_Apartment_25pctDuration,0) as leads_Apartment_25pctDuration,
# MAGIC coalesce(leads_Independent_Floor_25pctDuration,0) as leads_Independent_Floor_25pctDuration,
# MAGIC coalesce(leads_other_property_type_25pctDuration,0) as leads_other_property_type_25pctDuration,
# MAGIC
# MAGIC coalesce(listings_1RK1BHK_25pctDuration,0) as listings_1RK1BHK_25pctDuration,
# MAGIC coalesce(listings_2BHK_25pctDuration,0) as listings_2BHK_25pctDuration,
# MAGIC coalesce(listings_3BHK_25pctDuration,0) as listings_3BHK_25pctDuration,
# MAGIC coalesce(listings_3plusBHK_25pctDuration,0) as listings_3plusBHK_25pctDuration,
# MAGIC coalesce(listings_OtherBHKType_25pctDuration,0) as listings_OtherBHKType_25pctDuration,
# MAGIC coalesce(leads_1RK1BHK_25pctDuration,0) as leads_1RK1BHK_25pctDuration,
# MAGIC coalesce(leads_2BHK_25pctDuration,0) as leads_2BHK_25pctDuration,
# MAGIC coalesce(leads_3BHK_25pctDuration,0) as leads_3BHK_25pctDuration,
# MAGIC coalesce(leads_3plusBHK_25pctDuration,0) as leads_3plusBHK_25pctDuration,
# MAGIC coalesce(leads_OtherBHKType_25pctDuration,0) as leads_Others_25pctDuration,
# MAGIC
# MAGIC coalesce(churn_shadow_leads_25pctDuration,0) as churn_shadow_leads_25pctDuration,
# MAGIC coalesce(CR_ImageCR_ReCallback_leads_25pctDuration,0) as CR_ImageCR_ReCallback_leads_25pctDuration,
# MAGIC coalesce(call_ivr_leads_25pctDuration,0) as call_ivr_leads_25pctDuration,
# MAGIC coalesce(crm_leads_25pctDuration,0) as crm_leads_25pctDuration,
# MAGIC coalesce(fb_fbM_leads_25pctDuration,0) as fb_fbM_leads_25pctDuration,
# MAGIC coalesce(other_channel_leads_25pctDuration,0) as other_channel_leads_25pctDuration
# MAGIC
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_listings_features_prefinal 
# MAGIC group by all;
# MAGIC
# MAGIC select * from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_listings_features_final limit 5

# COMMAND ----------

# MAGIC %md 
# MAGIC ### City Level Features

# COMMAND ----------

# MAGIC %md
# MAGIC #### City Renewal Percentage Month Wise Feature

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_city_renewal_percentage
# MAGIC
# MAGIC with base as (
# MAGIC SELECT
# MAGIC   sr.city,  
# MAGIC   MONTH(sr.MAX_END_DATETIME) as renewal_due_month,
# MAGIC   count(sr.OPPORTUNITY_ID) as total_opportunities_cityLevel_monthly,
# MAGIC   sum(CASE WHEN nrr.RENEWAL_WINDOW IN ('45D Renewed') THEN 1 ELSE 0 END) as tot_renewed_opportunities_cityLevel_monthly
# MAGIC
# MAGIC FROM edw_product_catalog.product_derived.seller_retention_extended_renewal_window sr
# MAGIC INNER JOIN edw_product_catalog.product_derived.new_renewal_report nrr on sr.opportunity_id = nrr.opportunity_id 
# MAGIC LEFT JOIN sapna.opportunity_package_mappings_v2 opm ON opm.opportunity_id = sr.opportunity_id
# MAGIC WHERE LOWER(sr.profile_type) IN ('broker','builder')
# MAGIC and YEAR(sr.MAX_END_DATETIME) >= '2021'
# MAGIC and sr.city is not null
# MAGIC GROUP BY sr.city,MONTH(sr.MAX_END_DATETIME)
# MAGIC ORDER BY sr.city)
# MAGIC
# MAGIC
# MAGIC select city,renewal_due_month,
# MAGIC total_opportunities_cityLevel_monthly,
# MAGIC tot_renewed_opportunities_cityLevel_monthly,
# MAGIC case when total_opportunities_cityLevel_monthly = 0 then 0 else round(tot_renewed_opportunities_cityLevel_monthly*100/total_opportunities_cityLevel_monthly,2) end as renewal_pct_cityLevel_monthly
# MAGIC from base 
# MAGIC order by all;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### City Leads Listings Features

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_city_leads_listings_daily
# MAGIC              
# MAGIC with property_distribution_bhk_type_city as (
# MAGIC
# MAGIC                 select a.city,a.lead_date,
# MAGIC                 sum(case when bhk_type = '1RK/1BHK' then 1 else 0 end) as listings_1RK1BHK_city_daily,
# MAGIC                 sum(case when bhk_type = '2 BHK' then 1 else 0 end) as listings_2BHK_city_daily,
# MAGIC                 sum(case when bhk_type = '3 BHK' then 1 else 0 end) as listings_3BHK_city_daily,
# MAGIC                 sum(case when bhk_type = '3+ BHK' then 1 else 0 end) as listings_3plusBHK_city_daily,
# MAGIC                 sum(case when bhk_type = 'Others' then 1 else 0 end) as listings_bhk_Others_city_daily
# MAGIC                 from
# MAGIC                
# MAGIC                (select a.city,a.lead_date,a.bhk_type,a.listing_id
# MAGIC                 from  data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data a
# MAGIC                 group by 1,2,3,4) a
# MAGIC                 group by 1,2),
# MAGIC
# MAGIC leads_distribution_bhk_type_city as (
# MAGIC
# MAGIC                 select a.city,a.lead_date,
# MAGIC                 sum(case when bhk_type = '1RK/1BHK' then 1 else 0 end) as leads_1RK1BHK_city_daily,
# MAGIC                 sum(case when bhk_type = '2 BHK' then 1 else 0 end) as leads_2BHK_city_daily,
# MAGIC                 sum(case when bhk_type = '3 BHK' then 1 else 0 end) as leads_3BHK_city_daily,
# MAGIC                 sum(case when bhk_type = '3+ BHK' then 1 else 0 end) as leads_3plusBHK_city_daily,
# MAGIC                 sum(case when bhk_type = 'Others' then 1 else 0 end) as leads_bhk_Others_city_daily
# MAGIC                 from
# MAGIC                
# MAGIC                (select a.city,a.lead_date,a.listing_id,a.bhk_type,a.lead_generator_new,a.lead_id
# MAGIC                 from  data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data a
# MAGIC                 group by 1,2,3,4,5,6) a
# MAGIC                 group by 1,2),
# MAGIC
# MAGIC leads_listings_city as (
# MAGIC               select city,lead_date,
# MAGIC               count(lead_id) as total_leads_daily,
# MAGIC               count(distinct p.listing_id) as tot_listings_daily
# MAGIC      
# MAGIC               from 
# MAGIC                     (
# MAGIC                     select a.city,a.lead_date,
# MAGIC                     a.listing_id,
# MAGIC                     a.listing_price,
# MAGIC                     a.property_type,
# MAGIC                     a.lead_id,
# MAGIC                     a.lead_generator_new
# MAGIC                     from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data a
# MAGIC                     group by all
# MAGIC                     order by 1,2,3) p
# MAGIC               group by 1,2
# MAGIC               order by 1,2),
# MAGIC
# MAGIC sellers_listings_city as ( select a.city,a.lead_date,count(distinct a.seller_uuid) as active_sellers_daily
# MAGIC                     from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data a
# MAGIC                     group by 1,2
# MAGIC                     order by 1,2,3)
# MAGIC
# MAGIC select a.city,a.lead_date,
# MAGIC p.active_sellers_daily,
# MAGIC o.tot_listings_daily,
# MAGIC total_leads_daily,
# MAGIC listings_1RK1BHK_city_daily,
# MAGIC listings_2BHK_city_daily,
# MAGIC listings_3BHK_city_daily,
# MAGIC listings_3plusBHK_city_daily,
# MAGIC listings_bhk_Others_city_daily,
# MAGIC leads_1RK1BHK_city_daily,
# MAGIC leads_2BHK_city_daily,
# MAGIC leads_3BHK_city_daily,
# MAGIC leads_3plusBHK_city_daily,
# MAGIC leads_bhk_Others_city_daily
# MAGIC
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_base_data a
# MAGIC left join leads_distribution_bhk_type_city n  on a.city = n.city and a.lead_date = n.lead_date
# MAGIC left join property_distribution_bhk_type_city q on a.city = q.city and a.lead_date = q.lead_date
# MAGIC left join leads_listings_city o on a.city = o.city and a.lead_date = o.lead_date
# MAGIC left join sellers_listings_city p on a.city = p.city and a.lead_date = p.lead_date
# MAGIC where a.city is not null
# MAGIC group by all
# MAGIC order by city,lead_date
# MAGIC ;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_city_leads_listings_features
# MAGIC
# MAGIC with base as (
# MAGIC select a.city,
# MAGIC a.seller_uuid,
# MAGIC a.OPPORTUNITY_ID,
# MAGIC lead_date,
# MAGIC active_sellers_daily,
# MAGIC tot_listings_daily,
# MAGIC total_leads_daily,
# MAGIC listings_1RK1BHK_city_daily,
# MAGIC listings_2BHK_city_daily,
# MAGIC listings_3BHK_city_daily,
# MAGIC listings_3plusBHK_city_daily,
# MAGIC listings_bhk_Others_city_daily,
# MAGIC leads_1RK1BHK_city_daily,
# MAGIC leads_2BHK_city_daily,
# MAGIC leads_3BHK_city_daily,
# MAGIC leads_3plusBHK_city_daily,
# MAGIC leads_bhk_Others_city_daily,
# MAGIC
# MAGIC round(case when active_sellers_daily = 0 then 0 else (total_leads_daily*100/active_sellers_daily) end,2) as 
# MAGIC      leads_per_seller_daily,
# MAGIC
# MAGIC round(case when active_sellers_daily = 0 then 0 else (tot_listings_daily*100/active_sellers_daily) end,2) as 
# MAGIC      listings_per_seller_daily,
# MAGIC
# MAGIC round(case when tot_listings_daily = 0 then 0 else (total_leads_daily*100/tot_listings_daily) end,2) as 
# MAGIC      leads_per_listings_daily
# MAGIC
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data a
# MAGIC left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_city_leads_listings_daily b 
# MAGIC                                                        on a.city = b.city and b.lead_date>= a.oneFourth_cutOff_date-365 and b.lead_date <= a.oneFourth_cutOff_date
# MAGIC group by all
# MAGIC order by 1,2,3,4
# MAGIC
# MAGIC ) 
# MAGIC
# MAGIC select city,
# MAGIC seller_uuid,
# MAGIC OPPORTUNITY_ID,
# MAGIC
# MAGIC coalesce(round(median(active_sellers_daily),0),0) as active_sellers_city_median,
# MAGIC coalesce(round(median(tot_listings_daily),0),0) as tot_listings_city_median,
# MAGIC coalesce(round(median(listings_1RK1BHK_city_daily),0),0) listings_1RK1BHK_city_median,
# MAGIC coalesce(round(median(listings_2BHK_city_daily),0),0) listings_2BHK_city_median,
# MAGIC coalesce(round(median(listings_3BHK_city_daily),0),0) listings_3BHK_city_median,
# MAGIC coalesce(round(median(listings_bhk_Others_city_daily),0),0) listings_bhk_Others_city_median,
# MAGIC
# MAGIC
# MAGIC coalesce(round(median(total_leads_daily),0),0) total_leads_city_median,
# MAGIC coalesce(round(median(leads_1RK1BHK_city_daily),0),0) leads_1RK1BHK_city_median,
# MAGIC coalesce(round(median(leads_2BHK_city_daily),0),0) leads_2BHK_city_median,
# MAGIC coalesce(round(median(leads_3BHK_city_daily),0),0) leads_3BHK_city_median,
# MAGIC coalesce(round(median(leads_bhk_Others_city_daily),0),0) leads_bhk_Others_city_median,
# MAGIC
# MAGIC
# MAGIC
# MAGIC coalesce(round(median(leads_per_seller_daily),0),0) as leads_per_seller_city_median,
# MAGIC coalesce(round(median(listings_per_seller_daily),0),0) as listings_per_seller_city_median,
# MAGIC coalesce(round(median(leads_per_listings_daily),0),0) as leads_per_listings_city_median
# MAGIC
# MAGIC
# MAGIC from base 
# MAGIC group by all
# MAGIC order by 1,2,3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ticketing Strom features

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_strom_features
# MAGIC
# MAGIC with tickets_base as
# MAGIC
# MAGIC     (select lower(attribute_value) as seller_uuid,ticket_id,date(t.created_at) as date_ticket,
# MAGIC       mtc.project_id,mtp.name as project_name
# MAGIC     from storm.tickets t 
# MAGIC     left join storm.ticket_attributes ta on ta.ticket_id = t.id
# MAGIC     left join storm.master_ticket_category mtc on mtc.id = t.category_id
# MAGIC     left join storm.master_ticket_project mtp on mtp.id = mtc.project_id
# MAGIC     
# MAGIC     where mtc.project_id = 18 and 
# MAGIC     lower(ta.attribute_value) in 
# MAGIC                                 (select distinct(lower(seller_uuid)) as seller_uuid 
# MAGIC                                 from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data)),
# MAGIC
# MAGIC base_25pctDuration as (
# MAGIC select a.city,a.seller_uuid,a.OPPORTUNITY_ID,
# MAGIC coalesce(count(distinct b.ticket_id),0) as lead_delivery_ticket_storm_30D
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data a
# MAGIC left join tickets_base b on lower(a.seller_uuid) = lower(b.seller_uuid)
# MAGIC       and b.date_ticket >= a.package_start_date-10 and b.date_ticket <= a.oneFourth_cutOff_date
# MAGIC group by 1,2,3
# MAGIC order by 1,2,3)
# MAGIC
# MAGIC select a.city,a.seller_uuid,a.OPPORTUNITY_ID,
# MAGIC coalesce(lead_delivery_ticket_storm_30D,0) as lead_delivery_ticket_storm_25pctDuration
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data a 
# MAGIC left join base_25pctDuration t3 on a.SELLER_UUID = t3.seller_uuid and a.OPPORTUNITY_ID = t3.OPPORTUNITY_ID
# MAGIC group by all
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Housing Staff Connect feature

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC create or replace table data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_housing_staff_connect_features
# MAGIC
# MAGIC with base as 
# MAGIC
# MAGIC (select seller_uuid,opportunity_id,package_start_date,package_end_date,oneFourth_cutOff_date,
# MAGIC sbd.account_owner_id
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data sbd 
# MAGIC group by all),
# MAGIC
# MAGIC experience_365 as (
# MAGIC select z.account_owner_id,z.opportunity_id,
# MAGIC count(distinct z.previous_opportunity_id) as cnt_1year_oppor_accOwner_25pctDuration,
# MAGIC sum( z.Previous_Renewal_Flag) as cnt_1year_oppor_renewal_accOwner_25pctDuration
# MAGIC from 
# MAGIC
# MAGIC       (
# MAGIC       select b.account_owner_id,b.opportunity_id,b.package_start_date,b.package_end_date,
# MAGIC       sr.OPPORTUNITY_ID as previous_opportunity_id,
# MAGIC       CASE WHEN nrr.RENEWAL_WINDOW IN ('45D Renewed') THEN 1 ELSE 0 END as Previous_Renewal_Flag
# MAGIC       from base b 
# MAGIC       left join edw_product_catalog.product_derived.seller_retention_extended_renewal_window  sr on sr.account_owner_id = b.account_owner_id and date(sr.MAX_END_DATETIME) <= b.oneFourth_cutOff_date 
# MAGIC                                     and date(sr.MAX_END_DATETIME) >= b.oneFourth_cutOff_date-365 
# MAGIC       inner join edw_product_catalog.product_derived.new_renewal_report nrr on sr.opportunity_id = nrr.opportunity_id                            
# MAGIC       order by 1,2,3 desc 
# MAGIC       ) z 
# MAGIC group by 1,2)
# MAGIC
# MAGIC select a.account_owner_id,a.opportunity_id,
# MAGIC em.cnt_1year_oppor_accOwner_25pctDuration,
# MAGIC em.cnt_1year_oppor_renewal_accOwner_25pctDuration,
# MAGIC case when em.cnt_1year_oppor_accOwner_25pctDuration = 0 then 0 else round((em.cnt_1year_oppor_renewal_accOwner_25pctDuration*100/em.cnt_1year_oppor_accOwner_25pctDuration),2) end as renewal_pct_accOwner_1year_25pctDuration
# MAGIC from base a 
# MAGIC
# MAGIC left join experience_365 em on em.opportunity_id = a.opportunity_id and em.account_owner_id = a.account_owner_id
# MAGIC group by all;
# MAGIC
# MAGIC
# MAGIC select * from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_housing_staff_connect_features limit 4
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seller's activity on Competitors platform

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_competitors_features
# MAGIC
# MAGIC with crawled_data as 
# MAGIC
# MAGIC (select * 
# MAGIC from benchmark_db.competitive_data_generic b
# MAGIC where (cast(right(trim(b.mobile),10) as string) in (select distinct cast(phone_number as string) from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data) or
# MAGIC trim(lower(b.email)) in (select distinct lower(trim(email)) as email from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data where len(email)>5))),
# MAGIC
# MAGIC base_25pctDuration as (
# MAGIC select city,seller_uuid,OPPORTUNITY_ID,coalesce(count(listing_id),0) as cnt_listings_compPlatform_25pctDuration
# MAGIC
# MAGIC                 from (
# MAGIC                 select a.city,a.seller_uuid,a.OPPORTUNITY_ID,b.listing_posted_date,b.listing_id
# MAGIC                 from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data  a
# MAGIC
# MAGIC                  left join crawled_data b on  (cast(right(trim(b.mobile),10) as string) = cast(a.phone_number as string)
# MAGIC                         or lower(trim(b.email)) = lower(trim(a.email)))  and b.listing_posted_date >= a.package_start_date-90 and b.listing_posted_date <=a.oneFourth_cutOff_date
# MAGIC                 group by all
# MAGIC                 order by 1,2,3,4 ) x
# MAGIC group by 1,2,3)
# MAGIC
# MAGIC select a.city,a.seller_uuid,a.OPPORTUNITY_ID,
# MAGIC coalesce(b3.cnt_listings_compPlatform_25pctDuration,0) as cnt_listing_other_platforms_25pctDuration
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data a
# MAGIC left join base_25pctDuration b3 on a.seller_uuid = b3.seller_uuid and a.OPPORTUNITY_ID = b3.OPPORTUNITY_ID
# MAGIC group by all
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging all the features

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features_latest
# MAGIC
# MAGIC with base as (
# MAGIC select date(current_date()) as feature_creation_date,
# MAGIC sbd.city,
# MAGIC sbd.SELLER_UUID,
# MAGIC sbd.PROFILE_TYPE,
# MAGIC sbd.OPPORTUNITY_ID,
# MAGIC sbd.package_start_date,
# MAGIC sbd.oneFourth_cutOff_date,
# MAGIC sbd.package_end_date,
# MAGIC sbd.renewal_due_year,
# MAGIC sbd.renewal_due_month,
# MAGIC sbd.package_duration,
# MAGIC
# MAGIC m.total_opportunities_cityLevel_monthly,
# MAGIC m.tot_renewed_opportunities_cityLevel_monthly,
# MAGIC m.renewal_pct_cityLevel_monthly,
# MAGIC n.active_sellers_city_median,
# MAGIC n.tot_listings_city_median,
# MAGIC n.total_leads_city_median,
# MAGIC n.leads_per_seller_city_median,
# MAGIC n.listings_per_seller_city_median,
# MAGIC n.leads_per_listings_city_median,
# MAGIC n.listings_1RK1BHK_city_median,
# MAGIC n.listings_2BHK_city_median,
# MAGIC n.listings_3BHK_city_median,
# MAGIC n.listings_bhk_Others_city_median,
# MAGIC n.leads_1RK1BHK_city_median,
# MAGIC n.leads_2BHK_city_median,
# MAGIC n.leads_3BHK_city_median,
# MAGIC n.leads_bhk_Others_city_median,
# MAGIC
# MAGIC spp.seller_tot_packages_previous,
# MAGIC spp.seller_tot_renewal_previous,
# MAGIC case when spp.seller_tot_packages_previous = 0 then 0 else round((spp.seller_tot_renewal_previous/spp.seller_tot_packages_previous)*100,2) end as seller_previous_renewal_pct,
# MAGIC
# MAGIC sbd.rate_price,
# MAGIC sbd.sales_price,
# MAGIC round((sbd.rate_price-sbd.sales_price)*100/sbd.rate_price,1) as package_discount_pct,
# MAGIC round(sbd.sales_price/pqr.count_slots_pkg) as ratio_sales_price_slots,
# MAGIC pqr.count_slots_pkg,
# MAGIC ll.total_leads_25pctDuration,
# MAGIC ll.tot_listings_25pctDuration,
# MAGIC ll.tot_listing_price_25pctDuration,
# MAGIC
# MAGIC round(ll.tot_listings_25pctDuration/pqr.count_slots_pkg) as ratio_listings_slots_25pctDuration,
# MAGIC round(ll.total_leads_25pctDuration/pqr.count_slots_pkg) as ratio_leads_slots_25pctDuration,
# MAGIC round(ll.tot_listing_price_25pctDuration/sbd.sales_price) as ratio_listing_price_salesPrice_25pctDuration,
# MAGIC round(ll.tot_listings_25pctDuration/sbd.package_duration_oneFourth) as ratio_listings_pkg_duration_25pctDuration,
# MAGIC round(ll.total_leads_25pctDuration/sbd.package_duration_oneFourth) as ratio_leads_pkg_duration_25pctDuration,
# MAGIC
# MAGIC ll.listings_1RK1BHK_25pctDuration,
# MAGIC ll.listings_2BHK_25pctDuration,
# MAGIC ll.listings_3BHK_25pctDuration,
# MAGIC ll.listings_3plusBHK_25pctDuration,
# MAGIC ll.listings_OtherBHKType_25pctDuration,
# MAGIC ll.leads_1RK1BHK_25pctDuration,
# MAGIC ll.leads_2BHK_25pctDuration,
# MAGIC ll.leads_3BHK_25pctDuration,
# MAGIC ll.leads_3plusBHK_25pctDuration,
# MAGIC ll.leads_Others_25pctDuration,
# MAGIC ll.churn_shadow_leads_25pctDuration,
# MAGIC ll.CR_ImageCR_ReCallback_leads_25pctDuration,
# MAGIC ll.call_ivr_leads_25pctDuration,
# MAGIC ll.crm_leads_25pctDuration,
# MAGIC ll.fb_fbM_leads_25pctDuration,
# MAGIC ll.other_channel_leads_25pctDuration,
# MAGIC
# MAGIC cf.cnt_listing_other_platforms_25pctDuration,
# MAGIC sf.lead_delivery_ticket_storm_25pctDuration,
# MAGIC hcf.cnt_1year_oppor_accOwner_25pctDuration,
# MAGIC hcf.cnt_1year_oppor_renewal_accOwner_25pctDuration,
# MAGIC hcf.renewal_pct_accOwner_1year_25pctDuration
# MAGIC
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_opportunity_base_data sbd
# MAGIC
# MAGIC left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_city_renewal_percentage m on m.renewal_due_month = month(sbd.oneFourth_cutOff_date) and m.city = sbd.city
# MAGIC
# MAGIC left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_city_leads_listings_features n 
# MAGIC                  on  n.seller_uuid = sbd.SELLER_UUID and n.opportunity_id = sbd.OPPORTUNITY_ID
# MAGIC
# MAGIC left join data_science_metastore.seller_renewal_25pct_featureStore.seller_slots_package pqr 
# MAGIC                  on  pqr.seller_uuid = sbd.SELLER_UUID and pqr.opportunity_id = sbd.OPPORTUNITY_ID
# MAGIC
# MAGIC left join data_science_metastore.seller_renewal_25pct_featureStore.seller_previous_packages spp 
# MAGIC                  on spp.seller_uuid = sbd.SELLER_UUID and spp.opportunity_id = sbd.OPPORTUNITY_ID
# MAGIC
# MAGIC left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_leads_listings_features_final ll on ll.seller_uuid = sbd.SELLER_UUID 
# MAGIC                                    and ll.opportunity_id = sbd.OPPORTUNITY_ID 
# MAGIC
# MAGIC left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_competitors_features cf on cf.seller_uuid = sbd.SELLER_UUID 
# MAGIC                                    and cf.opportunity_id = sbd.OPPORTUNITY_ID  
# MAGIC
# MAGIC left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_strom_features sf on sf.seller_uuid = sbd.SELLER_UUID 
# MAGIC                                    and sf.opportunity_id = sbd.OPPORTUNITY_ID 
# MAGIC
# MAGIC left join data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_housing_staff_connect_features hcf on sbd.opportunity_id = hcf.opportunity_id 
# MAGIC                                    and sbd.account_owner_id = hcf.account_owner_id
# MAGIC
# MAGIC where sbd.city is not null
# MAGIC group by all
# MAGIC order by sbd.city,sbd.SELLER_UUID,sbd.OPPORTUNITY_ID,sbd.package_end_date desc) 
# MAGIC
# MAGIC
# MAGIC
# MAGIC select feature_creation_date,city,
# MAGIC SELLER_UUID,
# MAGIC PROFILE_TYPE,
# MAGIC OPPORTUNITY_ID,
# MAGIC package_start_date,
# MAGIC oneFourth_cutOff_date,
# MAGIC package_end_date,
# MAGIC renewal_due_year,
# MAGIC renewal_due_month,
# MAGIC package_duration,
# MAGIC
# MAGIC coalesce(total_opportunities_cityLevel_monthly,0) as total_opportunities_cityLevel_monthly,
# MAGIC coalesce(tot_renewed_opportunities_cityLevel_monthly,0) as tot_renewed_opportunities_cityLevel_monthly,
# MAGIC coalesce(renewal_pct_cityLevel_monthly,0) as renewal_pct_cityLevel_monthly,
# MAGIC coalesce(active_sellers_city_median,0) as active_sellers_city_median,
# MAGIC coalesce(tot_listings_city_median,0) as tot_listings_city_median,
# MAGIC coalesce(total_leads_city_median,0) as total_leads_city_median,
# MAGIC coalesce(leads_per_seller_city_median,0) as leads_per_seller_city_median,
# MAGIC coalesce(listings_per_seller_city_median,0) as listings_per_seller_city_median,
# MAGIC coalesce(leads_per_listings_city_median,0) as leads_per_listings_city_median,
# MAGIC coalesce(listings_1RK1BHK_city_median,0) as listings_1RK1BHK_city_median,
# MAGIC coalesce(listings_2BHK_city_median,0) as listings_2BHK_city_median,
# MAGIC coalesce(listings_3BHK_city_median,0) as listings_3BHK_city_median,
# MAGIC coalesce(listings_bhk_Others_city_median,0) as listings_bhk_Others_city_median,
# MAGIC coalesce(leads_1RK1BHK_city_median,0) as leads_1RK1BHK_city_median,
# MAGIC coalesce(leads_2BHK_city_median,0) as leads_2BHK_city_median,
# MAGIC coalesce(leads_3BHK_city_median,0) as leads_3BHK_city_median,
# MAGIC coalesce(leads_bhk_Others_city_median,0) as leads_bhk_Others_city_median,
# MAGIC coalesce(seller_tot_packages_previous,0) as seller_tot_packages_previous,
# MAGIC coalesce(seller_tot_renewal_previous,0) as seller_tot_renewal_previous,
# MAGIC coalesce(seller_previous_renewal_pct,0) as seller_previous_renewal_pct,
# MAGIC coalesce(rate_price,0) as rate_price,
# MAGIC coalesce(sales_price,0) as sales_price,
# MAGIC coalesce(package_discount_pct,0) as package_discount_pct,
# MAGIC coalesce(ratio_sales_price_slots,0) as ratio_sales_price_slots,
# MAGIC coalesce(count_slots_pkg,0) as count_slots_pkg,
# MAGIC coalesce(total_leads_25pctDuration,0) as total_leads_25pctDuration,
# MAGIC coalesce(tot_listings_25pctDuration,0) as tot_listings_25pctDuration,
# MAGIC coalesce(tot_listing_price_25pctDuration,0) as tot_listing_price_25pctDuration,
# MAGIC coalesce(ratio_listings_slots_25pctDuration,0) as ratio_listings_slots_25pctDuration,
# MAGIC coalesce(ratio_leads_slots_25pctDuration,0) as ratio_leads_slots_25pctDuration,
# MAGIC coalesce(ratio_listing_price_salesPrice_25pctDuration,0) as ratio_listing_price_salesPrice_25pctDuration,
# MAGIC coalesce(ratio_listings_pkg_duration_25pctDuration,0) as ratio_listings_pkg_duration_25pctDuration,
# MAGIC coalesce(ratio_leads_pkg_duration_25pctDuration,0) as ratio_leads_pkg_duration_25pctDuration,
# MAGIC coalesce(listings_1RK1BHK_25pctDuration,0) as listings_1RK1BHK_25pctDuration,
# MAGIC coalesce(listings_2BHK_25pctDuration,0) as listings_2BHK_25pctDuration,
# MAGIC coalesce(listings_3BHK_25pctDuration,0) as listings_3BHK_25pctDuration,
# MAGIC coalesce(listings_3plusBHK_25pctDuration,0) as listings_3plusBHK_25pctDuration,
# MAGIC coalesce(listings_OtherBHKType_25pctDuration,0) as listings_OtherBHKType_25pctDuration,
# MAGIC coalesce(leads_1RK1BHK_25pctDuration,0) as leads_1RK1BHK_25pctDuration,
# MAGIC coalesce(leads_2BHK_25pctDuration,0) as leads_2BHK_25pctDuration,
# MAGIC coalesce(leads_3BHK_25pctDuration,0) as leads_3BHK_25pctDuration,
# MAGIC coalesce(leads_3plusBHK_25pctDuration,0) as leads_3plusBHK_25pctDuration,
# MAGIC coalesce(leads_Others_25pctDuration,0) as leads_Others_25pctDuration,
# MAGIC coalesce(churn_shadow_leads_25pctDuration,0) as churn_shadow_leads_25pctDuration,
# MAGIC coalesce(CR_ImageCR_ReCallback_leads_25pctDuration,0) as CR_ImageCR_ReCallback_leads_25pctDuration,
# MAGIC coalesce(call_ivr_leads_25pctDuration,0) as call_ivr_leads_25pctDuration,
# MAGIC coalesce(crm_leads_25pctDuration,0) as crm_leads_25pctDuration,
# MAGIC coalesce(fb_fbM_leads_25pctDuration,0) as fb_fbM_leads_25pctDuration,
# MAGIC coalesce(other_channel_leads_25pctDuration,0) as other_channel_leads_25pctDuration,
# MAGIC coalesce(cnt_listing_other_platforms_25pctDuration,0) as cnt_listing_other_platforms_25pctDuration,
# MAGIC coalesce(lead_delivery_ticket_storm_25pctDuration,0) as lead_delivery_ticket_storm_25pctDuration,
# MAGIC coalesce(cnt_1year_oppor_accOwner_25pctDuration,0) as cnt_1year_oppor_accOwner_25pctDuration,
# MAGIC coalesce(cnt_1year_oppor_renewal_accOwner_25pctDuration,0) as cnt_1year_oppor_renewal_accOwner_25pctDuration,
# MAGIC coalesce(renewal_pct_accOwner_1year_25pctDuration,0) as renewal_pct_accOwner_1year_25pctDuration
# MAGIC from base
# MAGIC group by all;
# MAGIC
# MAGIC
# MAGIC
# MAGIC select * from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features_latest limit 5
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features_latest order by SELLER_UUID,OPPORTUNITY_ID desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),count(distinct seller_uuid) 
# MAGIC  as sellers,count(distinct opportunity_id) as distinct_opportunities
# MAGIC  from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features_latest
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creating Repository of features

# COMMAND ----------


# drop table if exists data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features_repository

# COMMAND ----------

# %sql
# create table if not exists data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features_repository
# select * from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features_latest

# COMMAND ----------

# MAGIC %sql
# MAGIC select feature_creation_date,count(distinct OPPORTUNITY_ID) as cnt
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features_repository
# MAGIC group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features_repository where feature_creation_date = date(current_date())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into table data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features_repository
# MAGIC (feature_creation_date,city,SELLER_UUID,PROFILE_TYPE,OPPORTUNITY_ID,package_start_date,oneFourth_cutOff_date,package_end_date,
# MAGIC renewal_due_year,renewal_due_month,package_duration,total_opportunities_cityLevel_monthly,
# MAGIC tot_renewed_opportunities_cityLevel_monthly,
# MAGIC renewal_pct_cityLevel_monthly,
# MAGIC active_sellers_city_median,
# MAGIC tot_listings_city_median,
# MAGIC total_leads_city_median,
# MAGIC leads_per_seller_city_median,
# MAGIC listings_per_seller_city_median,
# MAGIC leads_per_listings_city_median,
# MAGIC listings_1RK1BHK_city_median,
# MAGIC listings_2BHK_city_median,
# MAGIC listings_3BHK_city_median,
# MAGIC listings_bhk_Others_city_median,
# MAGIC leads_1RK1BHK_city_median,
# MAGIC leads_2BHK_city_median,
# MAGIC leads_3BHK_city_median,
# MAGIC leads_bhk_Others_city_median,
# MAGIC seller_tot_packages_previous,
# MAGIC seller_tot_renewal_previous,
# MAGIC seller_previous_renewal_pct,
# MAGIC rate_price,
# MAGIC sales_price,
# MAGIC package_discount_pct,
# MAGIC ratio_sales_price_slots,
# MAGIC count_slots_pkg,
# MAGIC total_leads_25pctDuration,
# MAGIC tot_listings_25pctDuration,
# MAGIC tot_listing_price_25pctDuration,
# MAGIC ratio_listings_slots_25pctDuration,
# MAGIC ratio_leads_slots_25pctDuration,
# MAGIC ratio_listing_price_salesPrice_25pctDuration,
# MAGIC ratio_listings_pkg_duration_25pctDuration,
# MAGIC ratio_leads_pkg_duration_25pctDuration,
# MAGIC listings_1RK1BHK_25pctDuration,
# MAGIC listings_2BHK_25pctDuration,
# MAGIC listings_3BHK_25pctDuration,
# MAGIC listings_3plusBHK_25pctDuration,
# MAGIC listings_OtherBHKType_25pctDuration,
# MAGIC leads_1RK1BHK_25pctDuration,
# MAGIC leads_2BHK_25pctDuration,
# MAGIC leads_3BHK_25pctDuration,
# MAGIC leads_3plusBHK_25pctDuration,
# MAGIC leads_Others_25pctDuration,
# MAGIC churn_shadow_leads_25pctDuration,
# MAGIC CR_ImageCR_ReCallback_leads_25pctDuration,
# MAGIC call_ivr_leads_25pctDuration,
# MAGIC crm_leads_25pctDuration,
# MAGIC fb_fbM_leads_25pctDuration,
# MAGIC other_channel_leads_25pctDuration,
# MAGIC cnt_listing_other_platforms_25pctDuration,
# MAGIC lead_delivery_ticket_storm_25pctDuration,
# MAGIC cnt_1year_oppor_accOwner_25pctDuration,
# MAGIC cnt_1year_oppor_renewal_accOwner_25pctDuration,
# MAGIC renewal_pct_accOwner_1year_25pctDuration) 
# MAGIC
# MAGIC select feature_creation_date,city,SELLER_UUID,PROFILE_TYPE,OPPORTUNITY_ID,package_start_date,oneFourth_cutOff_date,package_end_date,
# MAGIC renewal_due_year,renewal_due_month,package_duration,total_opportunities_cityLevel_monthly,
# MAGIC tot_renewed_opportunities_cityLevel_monthly,
# MAGIC renewal_pct_cityLevel_monthly,
# MAGIC active_sellers_city_median,
# MAGIC tot_listings_city_median,
# MAGIC total_leads_city_median,
# MAGIC leads_per_seller_city_median,
# MAGIC listings_per_seller_city_median,
# MAGIC leads_per_listings_city_median,
# MAGIC listings_1RK1BHK_city_median,
# MAGIC listings_2BHK_city_median,
# MAGIC listings_3BHK_city_median,
# MAGIC listings_bhk_Others_city_median,
# MAGIC leads_1RK1BHK_city_median,
# MAGIC leads_2BHK_city_median,
# MAGIC leads_3BHK_city_median,
# MAGIC leads_bhk_Others_city_median,
# MAGIC seller_tot_packages_previous,
# MAGIC seller_tot_renewal_previous,
# MAGIC seller_previous_renewal_pct,
# MAGIC rate_price,
# MAGIC sales_price,
# MAGIC package_discount_pct,
# MAGIC ratio_sales_price_slots,
# MAGIC count_slots_pkg,
# MAGIC total_leads_25pctDuration,
# MAGIC tot_listings_25pctDuration,
# MAGIC tot_listing_price_25pctDuration,
# MAGIC ratio_listings_slots_25pctDuration,
# MAGIC ratio_leads_slots_25pctDuration,
# MAGIC ratio_listing_price_salesPrice_25pctDuration,
# MAGIC ratio_listings_pkg_duration_25pctDuration,
# MAGIC ratio_leads_pkg_duration_25pctDuration,
# MAGIC listings_1RK1BHK_25pctDuration,
# MAGIC listings_2BHK_25pctDuration,
# MAGIC listings_3BHK_25pctDuration,
# MAGIC listings_3plusBHK_25pctDuration,
# MAGIC listings_OtherBHKType_25pctDuration,
# MAGIC leads_1RK1BHK_25pctDuration,
# MAGIC leads_2BHK_25pctDuration,
# MAGIC leads_3BHK_25pctDuration,
# MAGIC leads_3plusBHK_25pctDuration,
# MAGIC leads_Others_25pctDuration,
# MAGIC churn_shadow_leads_25pctDuration,
# MAGIC CR_ImageCR_ReCallback_leads_25pctDuration,
# MAGIC call_ivr_leads_25pctDuration,
# MAGIC crm_leads_25pctDuration,
# MAGIC fb_fbM_leads_25pctDuration,
# MAGIC other_channel_leads_25pctDuration,
# MAGIC cnt_listing_other_platforms_25pctDuration,
# MAGIC lead_delivery_ticket_storm_25pctDuration,
# MAGIC cnt_1year_oppor_accOwner_25pctDuration,
# MAGIC cnt_1year_oppor_renewal_accOwner_25pctDuration,
# MAGIC renewal_pct_accOwner_1year_25pctDuration
# MAGIC from data_science_metastore.seller_renewal_25pct_featureStore.seller_renewal_25pctDuration_features_latest
# MAGIC group by all ;

# COMMAND ----------


