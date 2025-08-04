SELECT
  date,
  country,
  device,
  COUNT(*) AS ad_count,
  SUM(totalEmissions) AS total_emissions,
  AVG(totalEmissions) AS avg_emissions
FROM {{ ref('stg_emissions') }}
GROUP BY 1, 2, 3
