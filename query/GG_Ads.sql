WITH concat AS (
-- Concat all Accounts 
    -- VuaNemUSD
    SELECT
        'VuaNemUSD' AS account,
        day,
        campaign,
        adgroup,
        region,
        impressions,
        clicks,
        cost / 1000000 AS cost, -- Cost on BigQuery is 1e6
        allconv,
        allconvvalue
    FROM
        `voltaic-country-280607.vnggusd2.GEO_PERFORMANCE_REPORT`
    UNION
    ALL
    -- DemvnUSD
    SELECT
        'DemvnUSD' AS account,
        day,
        campaign,
        adgroup,
        region,
        impressions,
        clicks,
        cost / 1000000 AS cost, -- Cost on BigQuery is 1e6
        allconv,
        allconvvalue
    FROM
        `voltaic-country-280607.vndemvn2.GEO_PERFORMANCE_REPORT`
    UNION
    ALL
    -- VuaNemTk01
    SELECT
        'VuaNemTk01' AS account,
        day,
        campaign,
        adgroup,
        region,
        impressions,
        clicks,
        cost / (1000000 * 23395) AS cost, -- Cost on BigQuery is 1e6, Normalized to USD
        allconv,
        allconvvalue
    FROM
        `voltaic-country-280607.vnggtk01.GEO_PERFORMANCE_REPORT`
)
-- Join concat'd table with Location Dimensions
SELECT
    concat.account,
    concat.day,
    concat.campaign,
    concat.adgroup,
    concat.impressions,
    concat.clicks,
    concat.cost,
    concat.allconv,
    concat.allconvvalue,
    Dim_Adwords_Locations.Name AS region
FROM
    concat
    LEFT JOIN `voltaic-country-280607.Digital_Analytics.Dim_Adwords_Locations` Dim_Adwords_Locations ON concat.region = Dim_Adwords_Locations.Criteria_ID