// Preview the IPinfo Lite table
SELECT
  *
FROM
  lite
LIMIT
  10;

// Get the top 5 countries with the most IP ranges in the dataset
SELECT
    country,
    country_code,
    COUNT(*) AS range_count
FROM lite
GROUP BY
    country,
    country_code
ORDER BY
    range_count DESC
LIMIT 5;

// Retrieve 10 distinct ASNs located in the US that have a known AS domain
SELECT DISTINCT
    asn,
    as_name,
    as_domain
FROM lite
WHERE
    country_code = 'US'         -- Filter for ASes located in the United States
    AND as_domain IS NOT NULL   -- Only include records with a known AS domain
LIMIT 10;

select * from lite where country_code = 'US' and start_ip LIKE '%.%' and AS_DOMAIN is not null limit 50;

select count(*) from lite where country_code = 'US' and start_ip LIKE '%.%' and AS_DOMAIN is not null;



SELECT DISTINCT
    asn,
    as_name,
    as_domain
FROM lite
WHERE
    country_code = 'US'         -- Filter for ASes located in the United States
    AND as_domain like '%datainmotion%'
LIMIT 10;

// Top 10 ASNs (by name) with the highest number of IPv4 address ranges
/*
only considering entries that have a non-null AS domain.
*/
SELECT
    asn,
    as_name,
    SUM(COUNT_ip(start_ip, end_ip)) AS num_ranges
FROM lite
WHERE
    start_ip LIKE '%.%'        -- Filter only IPv4 addresses
    AND as_domain IS NOT NULL  -- Ensure AS domain info is present
GROUP BY
    asn,
    as_name
ORDER BY
    num_ranges DESC
LIMIT 10;

// Get the top 10 countries with the highest number of IPv4 address ranges
SELECT
    country,
    SUM(COUNT_ip(start_ip, end_ip)) AS num_ranges
FROM lite
WHERE
    start_ip LIKE '%.%'        -- Include only IPv4 address ranges
GROUP BY
    country
ORDER BY
    num_ranges DESC
LIMIT 10;

// Retrieve all records where the AS domain is 'microsoft.com'
/*
This ensures we capture IP ranges where Microsoft operates under a different ASN or name
*/
SELECT start_ip, end_ip, country, country_code, continent, continent_code, asn, as_name, as_domain
FROM lite
WHERE as_domain = 'microsoft.com';  -- Filter for Microsoft-related IP ranges;

// Generate different firewall and server configurations for IP ranges within China (CN)
/*
The query converts IP ranges to CIDR format and creates Apache, Nginx, and iptables rules
*/
SELECT
  cidr.value AS cidr,  -- The CIDR representation of the IP range
  CONCAT('deny from ', cidr.value) AS apache_deny,  -- Apache deny rule
  CONCAT('allow ', cidr.value, ';') AS nginx_allow,  -- Nginx allow rule
  CONCAT(
    'iptables -A INPUT -s ',
    cidr.value,
    ' -j ACCEPT'
  ) AS iptables_accept  -- iptables rule to accept traffic from this CIDR range
FROM
  lite,
  TABLE(FLATTEN(RANGE_TO_CIDR(start_ip, end_ip))) AS cidr  -- Flattened CIDR ranges from start and end IPs
WHERE
  country_code = 'CN'  -- Filter for IP ranges in China
LIMIT 10;

// Generate different firewall and server configurations for IP ranges associated with ASN 'AS4134'
/*
The query converts IP ranges to CIDR format and creates Apache, Nginx, and iptables rules
*/
SELECT
  cidr.value AS cidr,  -- The CIDR representation of the IP range
  CONCAT('deny from ', cidr.value) AS apache_deny,  -- Apache deny rule
  CONCAT('allow ', cidr.value, ';') AS nginx_allow,  -- Nginx allow rule
  CONCAT(
    'iptables -A INPUT -s ',
    cidr.value,
    ' -j ACCEPT'
  ) AS iptables_accept  -- iptables rule to accept traffic from this CIDR range
FROM
  lite,
  TABLE(FLATTEN(RANGE_TO_CIDR(start_ip, end_ip))) AS cidr  -- Flatten the CIDR ranges from the IP range (start_ip, end_ip)
WHERE
  asn = 'AS4134'  -- Filter for records associated with ASN 'AS4134'
LIMIT 10;

// Enrich a fixed list of IP addresses with IPinfo Lite metadata 
/*
Enrich a fixed list of IP addresses with IPinfo Lite metadata using the GET_IP_LITE() function
*/
WITH logs AS (
    SELECT ip
    FROM (VALUES
        ('21.146.22.125'),
        ('56.200.20.153'),
        ('124.25.156.143'),
        ('148.167.153.224'),
        ('64.10.101.135'),
        ('205.203.92.170'),
        ('36.20.73.115'),
        ('133.158.33.54')
    ) AS v(ip)
)

SELECT *
FROM logs l
JOIN TABLE(public.GET_IP_LITE(l.ip));
;

// Lookup a single IP address information
/*
Lookup a single IP address information from the IPinfo Lite database
*/
-- Lookup a single IP address information
SELECT *
FROM TABLE(public.GET_IP_LITE('21.146.22.125'));

