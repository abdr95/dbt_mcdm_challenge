with
    bing as (
        select
            ad_id as ad_id,
            null as add_to_cart,
            adset_id as adset_id,
            campaign_id as campaign_id,
            channel as channel,
            clicks as clicks,
            null as comments,
            null as creative_id,
            date as date,
            null as engagements,
            imps as impressions,
            null as installs,
            null as likes,
            null as link_clicks,
            null as placement_id,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            null as purchase,
            null as registrations,
            revenue as revenue,
            null as shares,
            spend as spend,
            conv as total_conversions,
            null as video_views
        from {{ ref("src_ads_bing_all_data") }}
    ),

    facebook as (
        select
            ad_id as ad_id,
            add_to_cart as add_to_cart,
            adset_id as adset_id,
            campaign_id as campaign_id,
            channel as channel,
            clicks as clicks,
            comments as comments,
            creative_id as creative_id,
            date as date,
            null as engagements,
            impressions as impressions,
            mobile_app_install as installs,
            likes as likes,
            inline_link_clicks as link_clicks,
            null as placement_id,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            purchase as purchase,
            complete_registration as registrations,
            null as revenue,
            shares as shares,
            spend as spend,
            null as total_conversions,
            views as video_views
        from {{ ref("src_ads_creative_facebook_all_data") }}
    ),

    tiktok as (
        select 
            ad_id as ad_id,
            add_to_cart as add_to_cart,
            adgroup_id as adset_id,
            campaign_id as campaign_id,
            channel as channel,
            clicks as clicks,
            null as comments,
            null as creative_id,
            date as date,
            null as engagements,
            impressions as impressions,
            rt_installs as installs,
            null as likes, 
            null as link_clicks,
            null as placement_id,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            purchase as purchase,
            registrations as registrations,
            null as revenue,
            null as shares,
            spend as spend,
            conversions as total_conversions,
            video_views as video_views
        from {{ref("src_ads_tiktok_ads_all_data")}}
    ),

    twitter as (
        select 
            null as ad_id,
            null as add_to_cart,
            null as adset_id,
            campaign_id as campaign_id,
            channel as channel,
            clicks as clicks,
            comments as comments,
            null as creative_id,
            date as date,
            engagements as engagements,
            impressions as impressions,
            null as installs,
            likes as likes,
            url_clicks as link_clicks,
            null as placement_id,
            null as post_click_conversions,
            null as post_view_conversions,
            null as posts,
            null as purchase,
            null as registrations,
            null as revenue,
            null as shares,
            spend as spend,
            null as total_conversions,
            video_total_views as video_views
        from {{ref("src_promoted_tweets_twitter_all_data")}}
    ),

    paid_ads__basic_performance as (
        select 
            bing.*
        from bing

        UNION ALL

        select 
            facebook.*
        from facebook

        UNION ALL

        select 
            tiktok.*
        from tiktok

        UNION ALL

        select 
            twitter.*
        from twitter
    )

select * from paid_ads__basic_performance