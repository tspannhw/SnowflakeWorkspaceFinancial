// Get total case count by country
/*
Calculates the total number of cases by country, aggregated over time.
*/
SELECT   COUNTRY_REGION, SUM(CASES) AS Cases
FROM     ECDC_GLOBAL
GROUP BY COUNTRY_REGION;

// Change in mobility in over time
/*
Displays the change in visits to places like grocery stores and parks by date, location and location type for a sub-region (Alexandria) of a state (Virginia) of a country (United States).
*/
SELECT DATE,
       COUNTRY_REGION,
       PROVINCE_STATE,
       GROCERY_AND_PHARMACY_CHANGE_PERC,
       PARKS_CHANGE_PERC,
       RESIDENTIAL_CHANGE_PERC,
       RETAIL_AND_RECREATION_CHANGE_PERC,
       TRANSIT_STATIONS_CHANGE_PERC,
       WORKPLACES_CHANGE_PERC
FROM   GOOG_GLOBAL_MOBILITY_REPORT
WHERE  COUNTRY_REGION = 'United States'
  AND PROVINCE_STATE = 'Virginia'
  AND SUB_REGION_2 = 'Alexandria';

// Date-dependent case fatality ratio
/*
Calculate case-fatality ratio for a given date
*/
SELECT m.COUNTRY_REGION, m.DATE, m.CASES, m.DEATHS, m.DEATHS / m.CASES as CFR
FROM (SELECT COUNTRY_REGION, DATE, AVG(CASES) AS CASES, AVG(DEATHS) AS DEATHS
      FROM ECDC_GLOBAL
      GROUP BY COUNTRY_REGION, DATE) m
WHERE m.CASES > 0;

