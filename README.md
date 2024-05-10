# Crashdata-IA

Motor Vehicle Collisions Analysis in New York City
This project focuses on analyzing motor vehicle collisions in New York City using data provided by the New York City Police Department (NYPD). The goal is to gain insights into collision patterns, identify risk factors, and develop strategies to improve road safety and reduce the occurrence and impact of collisions.
Datasets
The project utilizes three main datasets:

Crashes Dataset: Contains information about motor vehicle collisions reported by the NYPD. Each row represents a unique collision event and includes details such as date, time, location, number of persons injured or killed, contributing factors, and vehicle types involved.
Vehicles Dataset: Provides additional details about the vehicles involved in each collision. Each row represents a vehicle involved in a crash and includes information such as vehicle type, make, model, year, registration state, damage details, and contributing factors specific to each vehicle.
Person Dataset: Contains information about individuals involved in motor vehicle collisions, including drivers, passengers, pedestrians, and cyclists. Each row represents a person involved in a crash and includes details such as the person's role, injury severity, age, gender, position in the vehicle, safety equipment used, and contributing factors specific to each individual.

Data Architecture
The project follows a data architecture that incorporates both an Online Transactional Processing (OLTP) database and an Online Analytical Processing (OLAP) data warehouse. The architecture ensures efficient data storage, retrieval, and analysis.
OLTP Database
The OLTP database, implemented using MySQL, serves as the primary source of transactional data. It consists of three main tables: crashes, vehicles, and person, which store the raw data from the respective datasets. The tables are linked through foreign key relationships to maintain data integrity and enable efficient querying.
OLAP Data Warehouse
The OLAP data warehouse is designed to support complex analytical queries and reporting. It follows a star schema design, with a central fact table (Crashes) surrounded by dimension tables (Location, Time, Vehicle, Person, ContributingFactors). The data warehouse is populated through an Extract, Transform, Load (ETL) process that extracts data from the OLTP database, transforms it to fit the dimensional model, and loads it into the data warehouse.
Data Cleaning and Preparation
The datasets underwent a thorough cleaning and preparation process to ensure data quality and consistency. The following steps were performed:

Handling missing values: Missing values were identified and appropriately handled based on the nature of the missing data and the analysis requirements.
Data type conversion: Relevant columns were converted to appropriate data types (e.g., date, time, numeric) to facilitate analysis and visualization.
Data standardization: Inconsistent or invalid values were standardized or removed to maintain data integrity.
Data integration: The datasets were integrated based on the unique collision ID to enable comprehensive analysis across crashes, vehicles, and persons involved.

ETL Process
The ETL process was implemented using Python and AWS Lambda functions to extract data from the OLTP database, transform it, and load it into the OLAP data warehouse. The following Lambda functions were developed:

Extract Lambda: Retrieves data from the crashes, vehicles, and person tables in the OLTP database and stores it in an S3 bucket as CSV files.
Transform Lambda: Reads the CSV files from the S3 bucket, performs data transformations, and writes the transformed data back to S3 as transformed CSV files. The transformations include data cleaning, data type conversion, and data standardization.
Load Lambda: Reads the transformed CSV files from S3 and loads the data into the corresponding tables in the OLAP data warehouse.

The Lambda functions are triggered based on a schedule or specific events to ensure regular updates of the data warehouse.
Analysis and Visualization
The project utilizes various data analysis and visualization techniques to uncover insights and patterns in the motor vehicle collision data. The following analyses were conducted:

Collision Hotspots: Identifying high-risk locations and areas with a high concentration of collisions using geospatial analysis and mapping techniques.
Contributing Factors: Analyzing the most common contributing factors to collisions, such as driver behavior, environmental conditions, and vehicle-related factors, to understand the key causes of crashes.
Temporal Patterns: Examining the temporal distribution of collisions, including trends over time, peak hours, and days of the week, to identify temporal risk factors and guide resource allocation.
Collision Severity: Investigating the severity of collisions based on the number of persons injured or killed, and identifying factors associated with high-severity crashes.
Collision Types: Analyzing the types of collisions based on the road users involved (e.g., pedestrians, cyclists, motorists) to understand the specific safety challenges faced by different road user groups.
Demographic Analysis: Exploring the demographic characteristics of individuals involved in collisions, such as age and gender, to identify high-risk groups and guide targeted interventions.

The project utilizes various data visualization tools and techniques, such as bar charts, heatmaps, geospatial maps, and interactive dashboards, to effectively communicate the findings and insights.
Repository Structure
The repository is organized as follows:

data - please refer https://drive.google.com/drive/folders/17oAzLJ2z3q4sTN_n_TrpPNZlgN8bA5hN?usp=sharing. for the datasets
Scripts: Contains the Lambda function code for the ETL process, SQL and notebooks.
visualizations/: Contains the generated visualizations.
reports/: Includes the final report and presentation summarizing the project findings and recommendations.
README.md: Provides an overview of the project, datasets, architecture, and analysis conducted.

Dependencies
The project requires the following dependencies:

Python 3.x
Pandas
NumPy
Matplotlib
Seaborn
Tableau (for interactive visualizations and dashboards)
AWS Lambda
AWS S3

Usage
To reproduce the analysis and generate the visualizations:

Clone the repository: git clone https://github.com/LokeshDondapati/Crashdata-IA.git
Install the required dependencies
Set up the AWS Lambda functions and configure the necessary AWS permissions and triggers.
Run the Jupyter notebooks in the notebooks/ directory to execute the data cleaning, analysis, and visualization code.
Explore the generated visualizations in the visualizations/ directory.
Review the final report and presentation in the reports/ directory for a summary of the project findings and recommendations.

Contributions
Contributions to this project are Goutham, Lokesh, Mridul, Akshit, Heny and ashritha. If you have any suggestions, improvements, or additional analyses to propose, please submit a pull request or open an issue on the repository.

