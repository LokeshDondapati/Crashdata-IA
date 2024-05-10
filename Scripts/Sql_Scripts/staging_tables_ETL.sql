-- Create staging table for crashes data
USE traffic_data_warehouse;
CREATE TABLE staging_crashes (
 CrashID INT,
 CrashDate DATE,
 CrashTime TIME,
 NumberOfPersonsInjured INT,
 NumberOfPersonsKilled INT,
 NumberOfPedestriansInjured INT,
 NumberOfPedestriansKilled INT,
 NumberOfCyclistsInjured INT,
 NumberOfCyclistsKilled INT,
 NumberOfMotoristsInjured INT,
 NumberOfMotoristsKilled INT,
 PRIMARY KEY (CrashID)
);
-- Create staging table for person data
CREATE TABLE staging_person (
 PersonKey INT PRIMARY KEY,
 PersonType VARCHAR(50),
 PersonInjury VARCHAR(50),
 PersonAge INT,
 PersonGender VARCHAR(10),
 EjectionStatus VARCHAR(50),
 BodyInjury VARCHAR(50),
 PositionInVehicle VARCHAR(100),
 SafetyEquipment VARCHAR(50),
 CrashID INT
);
-- Create staging table for vehicles data
CREATE TABLE staging_vehicles (
 VehicleKey INT PRIMARY KEY,
 VehicleType VARCHAR(50),
 VehicleMake VARCHAR(50),
 VehicleModel VARCHAR(50),
 VehicleYear INT,
 RegistrationState VARCHAR(50),
 CrashID INT
);
-- Create staging table for contributing factors data
CREATE TABLE staging_contributing_factors (
 ContributingFactorKey INT PRIMARY KEY,
 ContributingFactor VARCHAR(100)
);
Step 3: Create an ETL Stored Procedure
DELIMITER $$
CREATE PROCEDURE etl_process()
BEGIN
 -- Truncate the staging tables to remove existing data
 TRUNCATE TABLE staging_crashes;
 TRUNCATE TABLE staging_person;
 TRUNCATE TABLE staging_vehicles;
 TRUNCATE TABLE staging_contributing_factors;
 -- Extract and transform data from the source crashes table
 INSERT INTO staging_crashes (
 CrashID,
 CrashDate,
 CrashTime,
 NumberOfPersonsInjured,
 NumberOfPersonsKilled,
 NumberOfPedestriansInjured,
 NumberOfPedestriansKilled,
 NumberOfCyclistsInjured,
 NumberOfCyclistsKilled,
 NumberOfMotoristsInjured,
 NumberOfMotoristsKilled
 )
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
 crash_data.crashes c
 WHERE
 c.CRASH_DATE >= (SELECT MAX(CrashDate) FROM traffic_data_warehouse.Crashes);
 -- Extract and transform data from the source person table
 INSERT INTO staging_person (
 PersonKey,
 PersonType,
 PersonInjury,
 PersonAge,
 PersonGender,
 EjectionStatus,
 BodyInjury,
 PositionInVehicle,
 SafetyEquipment,
 CrashID
 )
 SELECT
 ROW_NUMBER() OVER (ORDER BY person_combination) AS PersonKey,
 p.PERSON_TYPE AS PersonType,
 p.PERSON_INJURY AS PersonInjury,
 p.PERSON_AGE AS PersonAge,
 p.PERSON_SEX AS PersonGender,
 p.EJECTION AS EjectionStatus,
 p.BODILY_INJURY AS BodyInjury,
 p.POSITION_IN_VEHICLE AS PositionInVehicle,
 p.SAFETY_EQUIPMENT AS SafetyEquipment,
 p.COLLISION_ID AS CrashID
 FROM
 (
 SELECT
 CONCAT(COALESCE(p.PERSON_TYPE, 'Unknown'), '|',
COALESCE(p.PERSON_INJURY, 'Unknown'), '|', COALESCE(CAST(p.PERSON_AGE AS
CHAR), 'Unknown'), '|', COALESCE(p.PERSON_SEX, 'Unknown'), '|', COALESCE(p.EJECTION,
'Unknown'), '|', COALESCE(p.BODILY_INJURY, 'Unknown'), '|',
COALESCE(p.POSITION_IN_VEHICLE, 'Unknown'), '|', COALESCE(p.SAFETY_EQUIPMENT,
'Unknown')) AS person_combination,
 p.PERSON_TYPE,
 p.PERSON_INJURY,
 p.PERSON_AGE,
 p.PERSON_SEX,
 p.EJECTION,
 p.BODILY_INJURY,
 p.POSITION_IN_VEHICLE,
 p.SAFETY_EQUIPMENT,
 p.COLLISION_ID
 FROM
 crash_data.person p
 WHERE
 p.COLLISION_ID IN (SELECT COLLISION_ID FROM crash_data.crashes WHERE
CRASH_DATE >= (SELECT MAX(CrashDate) FROM traffic_data_warehouse.Crashes))
 GROUP BY
 person_combination,
 p.PERSON_TYPE,
 p.PERSON_INJURY,
 p.PERSON_AGE,
 p.PERSON_SEX,
 p.EJECTION,
 p.BODILY_INJURY,
 p.POSITION_IN_VEHICLE,
 p.SAFETY_EQUIPMENT,
 p.COLLISION_ID
 ) p;
 -- Extract and transform data from the source vehicles table
 INSERT INTO staging_vehicles (
 VehicleKey,
 VehicleType,
 VehicleMake,
 VehicleModel,
 VehicleYear,
 RegistrationState,
 CrashID
 )
 SELECT
 ROW_NUMBER() OVER (ORDER BY vehicle_combination) AS VehicleKey,
 v.VEHICLE_TYPE AS VehicleType,
 v.VEHICLE_MAKE AS VehicleMake,
 v.VEHICLE_MODEL AS VehicleModel,
 v.VEHICLE_YEAR AS VehicleYear,
 v.STATE_REGISTRATION AS RegistrationState,
 v.COLLISION_ID AS CrashID
 FROM
 (
 SELECT
 CONCAT(COALESCE(v.VEHICLE_TYPE, 'Unknown'), '|',
COALESCE(v.VEHICLE_MAKE, 'Unknown'), '|', COALESCE(v.VEHICLE_MODEL, 'Unknown'),
'|', COALESCE(CAST(v.VEHICLE_YEAR AS CHAR), 'Unknown')) AS vehicle_combination,
 v.VEHICLE_TYPE,
 v.VEHICLE_MAKE,
 v.VEHICLE_MODEL,
 v.VEHICLE_YEAR,
 v.STATE_REGISTRATION,
 v.COLLISION_ID
 FROM
 crash_data.vehicles v
 WHERE
 v.COLLISION_ID IN (SELECT COLLISION_ID FROM crash_data.crashes WHERE
CRASH_DATE >= (SELECT MAX(CrashDate) FROM traffic_data_warehouse.Crashes))
 GROUP BY
 vehicle_combination,
 v.VEHICLE_TYPE,
 v.VEHICLE_MAKE,
 v.VEHICLE_MODEL,
 v.VEHICLE_YEAR,
 v.STATE_REGISTRATION,
 v.COLLISION_ID
 ) v;
 -- Extract and transform data from the source crashes table for contributing factors
 INSERT INTO staging_contributing_factors (
 ContributingFactorKey,
 ContributingFactor
 )
 SELECT
 ROW_NUMBER() OVER (ORDER BY CONTRIBUTING_FACTORS) AS
ContributingFactorKey,
 CONTRIBUTING_FACTORS AS ContributingFactor
 FROM
 (
 SELECT DISTINCT
 CONTRIBUTING_FACTORS
 FROM
 crash_data.crashes
 WHERE
 CONTRIBUTING_FACTORS IS NOT NULL AND
 CRASH_DATE >= (SELECT MAX(CrashDate) FROM traffic_data_warehouse.Crashes)
 ) c;
 -- Handle error scenarios and logging
 -- ...
END$$
DELIMITER ;