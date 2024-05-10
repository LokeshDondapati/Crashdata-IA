-- dw creation
CREATE DATABASE traffic_data_warehouse_IA_FINAL;

USE traffic_data_warehouse;

-- Dimension tables
CREATE TABLE Location (
    LocationKey INT PRIMARY KEY,
    Borough VARCHAR(50),
    ZipCode INT,
    Latitude FLOAT,
    Longitude FLOAT,
    StreetName VARCHAR(100),
    CrossStreetName VARCHAR(100)
);

CREATE TABLE Time (
    TimeKey INT PRIMARY KEY,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT,
    Hour INT
);

CREATE TABLE Vehicle (
    VehicleKey INT PRIMARY KEY,
    VehicleType VARCHAR(50),
    VehicleMake VARCHAR(50),
    VehicleModel VARCHAR(50),
    VehicleYear INT,
    RegistrationState VARCHAR(50)
);

CREATE TABLE Person (
    PersonKey INT PRIMARY KEY,
    PersonType VARCHAR(50),
    PersonInjury VARCHAR(50),
    PersonAge INT,
    PersonGender VARCHAR(10),
    EjectionStatus VARCHAR(50),
    BodyInjury VARCHAR(50),
    PositionInVehicle VARCHAR(100),
    SafetyEquipment VARCHAR(50)
);

CREATE TABLE ContributingFactors (
    ContributingFactorKey INT PRIMARY KEY,
    ContributingFactor VARCHAR(100)
);

-- FACT TABLE
CREATE TABLE Crashes (
    CrashID INT PRIMARY KEY,
    LocationKey INT,
    TimeKey INT,
    VehicleKey INT,
    PersonKey INT,
    ContributingFactorKey INT,
    NumberOfPersonsInjured INT,
    NumberOfPersonsKilled INT,
    NumberOfPedestriansInjured INT,
    NumberOfPedestriansKilled INT,
    NumberOfCyclistsInjured INT,
    NumberOfCyclistsKilled INT,
    NumberOfMotoristsInjured INT,
    NumberOfMotoristsKilled INT,
    FOREIGN KEY (LocationKey) REFERENCES Location(LocationKey),
    FOREIGN KEY (TimeKey) REFERENCES Time(TimeKey),
    FOREIGN KEY (VehicleKey) REFERENCES Vehicle(VehicleKey),
    FOREIGN KEY (PersonKey) REFERENCES Person(PersonKey),
    FOREIGN KEY (ContributingFactorKey) REFERENCES ContributingFactors(ContributingFactorKey)
);