select * from VW_MTANEARBY;


WITH valid_coordinates AS (
    SELECT 
        *,
        -- Convert string coordinates to numeric, handling empty strings
        CASE 
            WHEN VEHICLELOCATIONLATITUDE = '' OR VEHICLELOCATIONLATITUDE IS NULL THEN NULL
            ELSE TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE)
        END AS lat_numeric,
        CASE 
            WHEN VEHICLELOCATIONLONGITUDE = '' OR VEHICLELOCATIONLONGITUDE IS NULL THEN NULL
            ELSE TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE)
        END AS lon_numeric
    FROM DEMO.DEMO.ICYMTA
),
nearby_calculations AS (
    SELECT 
        a.*,
        b.VEHICLEREF AS nearby_vehicle,
        b.LINEREF AS nearby_line,
        b.lat_numeric AS nearby_lat,
        b.lon_numeric AS nearby_lon,
        
        -- Safe Haversine calculation (only for valid coordinates)
        CASE 
            WHEN a.lat_numeric IS NOT NULL AND a.lon_numeric IS NOT NULL 
                 AND b.lat_numeric IS NOT NULL AND b.lon_numeric IS NOT NULL
                 AND a.VEHICLEREF != b.VEHICLEREF  -- Don't compare vehicle to itself
            THEN HAVERSINE(a.lat_numeric, a.lon_numeric, b.lat_numeric, b.lon_numeric)
            ELSE NULL
        END AS distance_km
    FROM valid_coordinates a
    CROSS JOIN valid_coordinates b
    WHERE a.lat_numeric IS NOT NULL 
      AND a.lon_numeric IS NOT NULL
      AND b.lat_numeric IS NOT NULL 
      AND b.lon_numeric IS NOT NULL
      AND a.VEHICLEREF != b.VEHICLEREF
)
SELECT 
    VEHICLEREF,
    LINEREF,
    STOPPOINTREF,
    PUBLISHEDLINENAME,
    lat_numeric AS vehicle_latitude,
    lon_numeric AS vehicle_longitude,
    RECORDEDATTIME,
    nearby_vehicle,
    nearby_line,
    nearby_lat,
    nearby_lon,
    distance_km,
    RANK() OVER (PARTITION BY VEHICLEREF ORDER BY distance_km ASC) AS proximity_rank
FROM nearby_calculations
WHERE distance_km IS NOT NULL 
  AND distance_km <= 5.0  -- Within 5km
ORDER BY VEHICLEREF, distance_km;