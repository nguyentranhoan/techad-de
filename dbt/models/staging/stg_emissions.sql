SELECT
  date,
  domain,
  format,
  country,
  device,
  totalEmissions,
  adSelectionEmissions,
  creativeDistributionEmissions,
  mediaDistributionEmissions,
  domainCoverage
FROM {{ source('emissions', 'clean_emissions') }}
