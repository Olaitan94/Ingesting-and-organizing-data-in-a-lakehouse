# Ingesting-and-organizing-data-in-a-lakehouse

## Background
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
- trains the user to do a STEDI balance exercise;
- has sensors on the device that collect data that can be used to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

The company has several customers who have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. **Privacy will be a primary consideration in deciding what data can be used. Hence, only data from customers who have agreed to share their data for research purposes will be used to train the ML model.**

## Task 

The team needs data from the STEDI Step Trainer sensors and the mobile app extracted and curated into a data lakehouse on AWS. by doing this, the data will be available for the Data scientists to train ML models.

## Data
1. Customer Records: JSON file which ocntains customer details like name, email, serialnumber and whether or not they have opted to share their data for research
2. Step trainer records: JSON files which contains data obtained from the step trainer like the serial number and the time the sensor reading was obtained.
3. Accelerometer Records which contains info like the user email, time the record was obtained and xyz readings.

## Solution 

- Storage: I have used AWS S3 bucket to store data and the data have been categorized into either the landing, trusted, or curated zone
  - Landing zones: This serves as a staging area where raw data from various sources is collected before and processing occurs.
  - Trusted zones: This zone contains data that has been cleaned and is ready for further analysis
  - Curated zones:  This zone is used to store data that has undergone further transformation, to meet the specific needs e.g ML in this case.
 
## ETL

I have used AWS Glue to run spark jobs for processing the data into the different zones
Below are the scripts used for ETL on Glue:
My data lakehouse solution is comprised of five Python scripts which are run in AWS Glue. The scripts are run in the following order:

Customer_Landing_to_Trusted.py: This script transfers customer data from the 'landing' to 'trusted' zones. It filters for customers who have agreed to share data with researchers.
Accelerometer_Landing_to_Trusted.py: This script transfers accelerometer data from the 'landing' to 'trusted' zones. It filters for Accelerometer readings from customers who have agreed to share data with researchers.
Customer_Trusted_to_Curated.py: This script transfers customer data from the 'trusted' to 'curated' zones. It filters for customers with Accelerometer readings and have agreed to share data with researchers.
Step_Trainer_Landing_to_Curated.py: This script transfers step trainer data from the 'landing' to 'curated' zones. It filters for curated customers with Step Trainer readings.
Machine_Learning_Curated.py: This script combines Step Trainer and Accelerometer data from the 'curated' zone into a single table to train a machine learning model.
