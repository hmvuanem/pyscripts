WITH
-- UNNEST actions to get actions for selected action_type
actions_unnest AS(
    SELECT
        DISTINCT date_start,
        campaign_id,
        adset_id,
        ad_id,
        region,
        CASE
            WHEN actions.value.action_type = 'offsite_conversion.fb_pixel_purchase' THEN actions.value.value
            ELSE NULL
        END AS offline_conversion_purchase,
        CASE
            WHEN actions.value.action_type = 'onsite_conversion.messaging_first_reply' THEN actions.value.value
            ELSE NULL
        END AS new_messaging_conversations,
        CASE
            WHEN actions.value.action_type = 'comment' THEN actions.value.value
            ELSE NULL
        END AS comments
    FROM
        `voltaic-country-280607.vnfbusd2.ads_insights_region` ads_insights
        CROSS JOIN UNNEST(actions) AS actions
    ORDER BY
        date_start,
        campaign_id,
        adset_id,
        ad_id,
        region
),

-- UNNEST action_values to get action_values for selected action_type
action_values_unnest AS (
    SELECT
        DISTINCT date_start,
        campaign_id,
        adset_id,
        ad_id,
        region,
        CASE
            WHEN action_values.value.action_type = 'offsite_conversion.fb_pixel_purchase' THEN action_values.value.value
            ELSE NULL
        END AS offline_conversion_purchase_value
    FROM
        `voltaic-country-280607.vnfbusd2.ads_insights_region` ads_insights
        CROSS JOIN UNNEST(action_values) AS action_values
    ORDER BY
        date_start,
        campaign_id,
        adset_id,
        ad_id,
        region
)

-- Final query with LEFT JOIN to UNNESTed query
SELECT
    ads_insights_region.date_start,
    ads_insights_region.campaign_name,
    ads_insights_region.adset_name,
    ads_insights_region.ad_name,
    ads_insights_region.region,
    ads_insights_region.reach,
    ads_insights_region.cpm,
    ads_insights_region.frequency,
    ads_insights_region.objective,
    ads_insights_region.spend,
    -- actions
    actions_unnest.offline_conversion_purchase,
    actions_unnest.new_messaging_conversations,
    actions_unnest.comments,
    -- action_values
    action_values_unnest.offline_conversion_purchase_value,
    -- ranking
    ads_insights.conversion_rate_ranking,
    ads_insights.engagement_rate_ranking,
    ads_insights.quality_ranking
FROM
    `voltaic-country-280607.vnfbusd2.ads_insights_region` ads_insights_region
    -- actions
    LEFT JOIN actions_unnest ON ads_insights_region.date_start = actions_unnest.date_start
    AND ads_insights_region.campaign_id = actions_unnest.campaign_id
    AND ads_insights_region.adset_id = actions_unnest.adset_id
    AND ads_insights_region.ad_id = actions_unnest.ad_id
    AND ads_insights_region.region = actions_unnest.region
    -- action_values
    LEFT JOIN action_values_unnest ON ads_insights_region.date_start = action_values_unnest.date_start
    AND ads_insights_region.campaign_id = action_values_unnest.campaign_id
    AND ads_insights_region.adset_id = action_values_unnest.adset_id
    AND ads_insights_region.ad_id = action_values_unnest.ad_id
    AND ads_insights_region.region = action_values_unnest.region
    -- ranking
    LEFT JOIN `voltaic-country-280607.vnfbusd2.ads_insights` ads_insights ON ads_insights_region.date_start = ads_insights.date_start
    AND ads_insights_region.campaign_id = ads_insights.campaign_id
    AND ads_insights_region.adset_id = ads_insights.adset_id
    AND ads_insights_region.ad_id = ads_insights.ad_id
ORDER BY
    ads_insights_region.date_start DESC,
    ads_insights_region.campaign_name,
    ads_insights_region.adset_name,
    ads_insights_region.ad_name,
    ads_insights_region.region