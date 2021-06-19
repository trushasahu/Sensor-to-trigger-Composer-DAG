# Sensor-to-trigger-Composer-DAG
Composer DAG triggered by Sensor when a CSV file placed into the triggered landing storage bucket path and load CSV data into BigQuery table

# Steps to load CSV to BQ table

### Create one landing buckets for Composer Sensor and place a CSV file
i.e. trigger/landing bucket for Composer Sensor i.e. third-campus-303308-cf-landing

### Create a Cloud Composer instance from console.

### Place the sensor-csv-to-bigquery.py file in the dag folder.
The dag folder is created in the cloud storage during Composer instance creation.

### Click on the Airflow link i.e. under 'Airflow webserver' tab of the Composer instance.

### You will find a new dag instance(sensor_gcs_to_bigquery) for the new file placed in the cloud storage dag folder.

### Trigger dag manually for first time to exeute the dag to load the csv into bigquer table.
The dag will be started and the sensor will look for the bigmart_data.csv file in the landing bucket. If found then it will load the CSV data into bigquery table and then again exeute the same tag to look for the same file to proceed again.

### Check in the BQ table for the data post completion of the dag process successfully.