DELIMITER $$
CREATE PROCEDURE update_dw_crashes()
BEGIN
 -- Delete existing records from the data warehouse crashes table
 -- based on the CrashID present in the staging table
 DELETE FROM Crashes
 WHERE CrashID IN (
 SELECT CrashID
 FROM staging_crashes
 );
 -- Insert new records from the staging table into the data warehouse crashes table
 INSERT INTO Crashes (
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
 FROM
 staging_crashes;

END$$
DELIMITER ;


DELIMITER $$
CREATE PROCEDURE update_dw_person()
BEGIN
 -- Delete existing records from the data warehouse person table
 -- based on the PersonKey present in the staging table
 DELETE FROM Person
 WHERE PersonKey IN (
 SELECT PersonKey
 FROM staging_person
 );
 -- Insert new records from the staging table into the data warehouse person table
 INSERT INTO Person (
 PersonKey,
 PersonType,
 PersonInjury,
 PersonAge,
 PersonGender,
 EjectionStatus,
 BodyInjury,
 PositionInVehicle,
 SafetyEquipment
 )
 SELECT
 PersonKey,
 PersonType,
 PersonInjury,
 PersonAge,
 PersonGender,
 EjectionStatus,
 BodyInjury,
 PositionInVehicle,
 SafetyEquipment
 FROM
 staging_person;

END$$
DELIMITER ;
Update Vehicle Table
DELIMITER $$
CREATE PROCEDURE update_dw_vehicle()
BEGIN
 -- Delete existing records from the data warehouse vehicle table
 -- based on the VehicleKey present in the staging table
 DELETE FROM Vehicle
 WHERE VehicleKey IN (
 SELECT VehicleKey
 FROM staging_vehicles
 );
 -- Insert new records from the staging table into the data warehouse vehicle table
 INSERT INTO Vehicle (
 VehicleKey,
 VehicleType,
 VehicleMake,
 VehicleModel,
 VehicleYear,
 RegistrationState
 )
 SELECT
 VehicleKey,
 VehicleType,
 VehicleMake,
 VehicleModel,
 VehicleYear,
 RegistrationState
 FROM
 staging_vehicles;

END$$
DELIMITER ;


DELIMITER $$
CREATE PROCEDURE update_dw_contributing_factors()
BEGIN
 -- Delete existing records from the data warehouse contributing factors table
 -- based on the ContributingFactorKey present in the staging table
 DELETE FROM ContributingFactors
 WHERE ContributingFactorKey IN (
 SELECT ContributingFactorKey
 FROM staging_contributing_factors
 );
 -- Insert new records from the staging table into the data warehouse contributing factors

 INSERT INTO ContributingFactors (
 ContributingFactorKey,
 ContributingFactor
 )
 SELECT
 ContributingFactorKey,
 ContributingFactor
 FROM
 staging_contributing_factors;

END$$
DELIMITER ;