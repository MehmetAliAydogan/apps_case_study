
WITH installs as (
SELECT FORMAT_DATETIME('%F', installed_at) as installed_date,
CASE WHEN network_name = 'TikTok' then 'Tiktok' 
WHEN network_name = 'Google' then 'Google Ads' 
WHEN network_name = 'ironSrc' then 'IronSource' 
else network_name end as network_name,
campaign_name,
user_id
FROM `example.installs`
), installs_grouped as (
  select installed_date, network_name, campaign_name, count(*) as install_count
  from installs
  group by installed_date, network_name, campaign_name
), cost_table as (
  SELECT FORMAT_DATETIME('%F', date) as cost_date,
  network_name, campaign_name, cost
  FROM `example.cost`
), cost_grouped as (
  select cost_date, network_name, campaign_name, sum(cost) as cost
  from cost_table
  group by cost_date, network_name, campaign_name
),
events as (
  SELECT FORMAT_DATETIME('%F', event_ts) as event_date,
  user_id,
  event_name,
  ad_revenue,
  ad_type
  from `example.events`
  where event_name in ('AdImpression', 'GameStart')
), events_joined as (
  select * from events
  left join installs using(user_id)
), grouped_events as (
  select 
  installed_date,
  network_name,
  campaign_name,
  count(case when event_name = 'GameStart' then 1 else 0 end) as game_start_count,
  sum(case when event_name = 'AdImpression' and ad_type='banner' then ad_revenue else 0 end) as banner_revenue,
  sum(case when event_name = 'AdImpression' and ad_type='interstitial' then ad_revenue else 0 end) as interstitial_revenue,
  sum(case when event_name = 'AdImpression' and ad_type='rewarded' then ad_revenue else 0 end) as rewarded_revenue
  from events_joined
  group by events_joined.installed_date, events_joined.network_name, events_joined.campaign_name
), final_table as (
  select grouped_events.installed_date, grouped_events.network_name, grouped_events.campaign_name,
   coalesce(install_count, 0) as install_count,
   coalesce(cost, 0) as cost,
   game_start_count,
   banner_revenue,
   interstitial_revenue,
   rewarded_revenue
  from grouped_events
  left join cost_grouped on grouped_events.installed_date = cost_grouped.cost_date and grouped_events.network_name = cost_grouped.network_name and grouped_events.campaign_name = cost_grouped.campaign_name
  left join installs_grouped on grouped_events.installed_date = installs_grouped.installed_date and grouped_events.network_name = installs_grouped.network_name and grouped_events.campaign_name = installs_grouped.campaign_name
) select * from final_table




