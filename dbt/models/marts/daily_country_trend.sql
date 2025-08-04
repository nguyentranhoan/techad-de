SELECT
  date,
  country,
  SUM(totalEmissions) AS daily_total_emissions
FROM {{ ref('stg_emissions') }}
GROUP BY date, country
ORDER BY date, country
