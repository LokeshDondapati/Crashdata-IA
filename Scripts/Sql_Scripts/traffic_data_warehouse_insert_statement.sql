



-- location dimention insert statement

INSERT INTO Location (LocationKey, Borough, ZipCode, Latitude, Longitude, StreetName, CrossStreetName)
WITH cte AS (
    SELECT
        CONCAT(COALESCE(c.BOROUGH, 'Unknown'), '|', COALESCE(CAST(c.ZIP_CODE AS CHAR), 'Unknown'), '|', COALESCE(CAST(c.LATITUDE AS CHAR), 'Unknown'), '|', COALESCE(CAST(c.LONGITUDE AS CHAR), 'Unknown'), '|', COALESCE(c.ON_STREET_NAME, 'Unknown'), '|', COALESCE(c.CROSS_STREET_NAME, 'Unknown')) AS location_combination,
        COALESCE(c.BOROUGH, 'Unknown') AS Borough,
        COALESCE(c.ZIP_CODE, 0) AS ZipCode,
        c.LATITUDE,
        c.LONGITUDE,
        c.ON_STREET_NAME,
        c.CROSS_STREET_NAME
    FROM
        crash_data.crashes c
    GROUP BY
        location_combination
)
SELECT
    ROW_NUMBER() OVER (ORDER BY location_combination) AS LocationKey,
    Borough,
    ZipCode,
    Latitude,
    Longitude,
    ON_STREET_NAME AS StreetName,
    CROSS_STREET_NAME AS CrossStreetName
FROM
    cte;


-- person dimention insert 
INSERT INTO Person (PersonKey, PersonType, PersonInjury, PersonAge, PersonGender, EjectionStatus, BodyInjury, PositionInVehicle, SafetyEquipment)
SELECT
    ROW_NUMBER() OVER (ORDER BY person_combination) AS PersonKey,
    p.PERSON_TYPE AS PersonType,
    p.PERSON_INJURY AS PersonInjury,
    p.PERSON_AGE AS PersonAge,
    p.PERSON_SEX AS PersonGender,
    p.EJECTION AS EjectionStatus,
    p.BODILY_INJURY AS BodyInjury,
    p.POSITION_IN_VEHICLE AS PositionInVehicle,
    p.SAFETY_EQUIPMENT AS SafetyEquipment
FROM
    (
        SELECT
            CONCAT(COALESCE(p.PERSON_TYPE, 'Unknown'), '|', COALESCE(p.PERSON_INJURY, 'Unknown'), '|', COALESCE(CAST(p.PERSON_AGE AS CHAR), 'Unknown'), '|', COALESCE(p.PERSON_SEX, 'Unknown'), '|', COALESCE(p.EJECTION, 'Unknown'), '|', COALESCE(p.BODILY_INJURY, 'Unknown'), '|', COALESCE(p.POSITION_IN_VEHICLE, 'Unknown'), '|', COALESCE(p.SAFETY_EQUIPMENT, 'Unknown')) AS person_combination,
            p.PERSON_TYPE,
            p.PERSON_INJURY,
            p.PERSON_AGE,
            p.PERSON_SEX,
            p.EJECTION,
            p.BODILY_INJURY,
            p.POSITION_IN_VEHICLE,
            p.SAFETY_EQUIPMENT
        FROM
            crash_data.person p
        GROUP BY
            person_combination,
            p.PERSON_TYPE,
            p.PERSON_INJURY,
            p.PERSON_AGE,
            p.PERSON_SEX,
            p.EJECTION,
            p.BODILY_INJURY,
            p.POSITION_IN_VEHICLE,
            p.SAFETY_EQUIPMENT
    ) p
ORDER BY
    person_combination;
    
    -- time dimention insert
    
    INSERT INTO Time (TimeKey, Year, Quarter, Month, Day, Hour)
SELECT
    CONCAT(YEAR(c.CRASH_DATE), LPAD(MONTH(c.CRASH_DATE), 2, '0'), LPAD(DAY(c.CRASH_DATE), 2, '0'), LPAD(HOUR(c.CRASH_TIME), 2, '0')) AS TimeKey,
    YEAR(c.CRASH_DATE) AS Year,
    QUARTER(c.CRASH_DATE) AS Quarter,
    MONTH(c.CRASH_DATE) AS Month,
    DAY(c.CRASH_DATE) AS Day,
    HOUR(c.CRASH_TIME) AS Hour
FROM
    crash_data.crashes c
GROUP BY
    TimeKey;
    
    
    -- ContributingFactors dimention insert
    
    INSERT INTO ContributingFactors (ContributingFactorKey, ContributingFactor)
SELECT
    ROW_NUMBER() OVER (ORDER BY CONTRIBUTING_FACTORS) AS ContributingFactorKey,
    CONTRIBUTING_FACTORS AS ContributingFactor
FROM
    (
        SELECT DISTINCT
            CONTRIBUTING_FACTORS
        FROM
            crash_data.crashes
        WHERE
            CONTRIBUTING_FACTORS IS NOT NULL
    ) c;
    
    -- vehicle dimention insert 
    
    INSERT INTO Vehicle (VehicleKey, VehicleType, VehicleMake, VehicleModel, VehicleYear, RegistrationState)
WITH cte AS (
    SELECT
        CONCAT(COALESCE(v.VEHICLE_TYPE, 'Unknown'), '|', COALESCE(v.VEHICLE_MAKE, 'Unknown'), '|', COALESCE(v.VEHICLE_MODEL, 'Unknown'), '|', COALESCE(CAST(v.VEHICLE_YEAR AS CHAR), 'Unknown')) AS vehicle_combination,
        v.VEHICLE_TYPE,
        v.VEHICLE_MAKE,
        v.VEHICLE_MODEL,
        v.VEHICLE_YEAR,
        v.STATE_REGISTRATION
    FROM
        crash_data.vehicles v
    GROUP BY
        vehicle_combination
)
SELECT
    ROW_NUMBER() OVER (ORDER BY vehicle_combination) AS VehicleKey,
    VEHICLE_TYPE as VehicleType,
    VEHICLE_MAKE as VehicleMake,
    VEHICLE_MODEL as VehicleModel,
    VEHICLE_YEAR as VehicleYear,
    STATE_REGISTRATION as RegistrationState
FROM
    cte;
    
    -- crash fact table insert
    
    INSERT INTO Crashes (CrashID, CrashDate, CrashTime, NumberOfPersonsInjured, NumberOfPersonsKilled, NumberOfPedestriansInjured, NumberOfPedestriansKilled, NumberOfCyclistsInjured, NumberOfCyclistsKilled, NumberOfMotoristsInjured, NumberOfMotoristsKilled)
SELECT
    c.COLLISION_ID,
    c.CRASH_DATE,
    c.CRASH_TIME,
    c.NUMBER_OF_PERSONS_INJURED,
    c.NUMBER_OF_PERSONS_KILLED,
    c.NUMBER_OF_PEDESTRIANS_INJURED,
    c.NUMBER_OF_PEDESTRIANS_KILLED,
    c.NUMBER_OF_CYCLIST_INJURED,
    c.NUMBER_OF_CYCLIST_KILLED,
    c.NUMBER_OF_MOTORIST_INJURED,
    c.NUMBER_OF_MOTORIST_KILLED
FROM
    crash_data.crashes c;
    
    
