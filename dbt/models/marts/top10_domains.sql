SELECT
  domain,
  SUM(totalEmissions) AS total_emissions
FROM {{ ref('stg_emissions') }}
GROUP BY domain
ORDER BY total_emissions DESC
LIMIT 10
