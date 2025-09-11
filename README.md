# Zillow End to End Data Engineering Project   
  
   
![DE pipeline image](https://github.com/user-attachments/assets/8aa05580-a80a-4d89-829e-95b079e67ebf)


The Airflow runs on the EC2 server which run's the dag, where the first task is extracting the data from the Rapid API using the python operator.   
  
-> Using Apache Airflow, the data is then transferred from the EC2 server to an AWS S3 bucket, serving as the main storage for the original data.   
-> A Lambda function moves the file from the main S3 bucket to a secondary S3 bucket i.e copy_data bucket, designated for further transformations.       
-> When the file is transfered to the copy_data bucket, another lambda function is triggered and this lambda function contains the transform_data code, which further transform the data and store it in the new S3 bucket in the csv format.    
-> Once the transformation is successful, Airflow runs an S3 Key Sensor task. It checks for the presence of the transformed file in the transform_data bucket. If the file is found, the file is then transferred to the Redshift data warehouse.  
-> With the data successfully loaded into Redshift, Amazon QuickSight is connected to the data warehouse for in-depth analysis and visualization.  
  
Here are some visualizations.  

![visualize1](https://github.com/mrtanush18/real-estate-data-pipeline/blob/main/2.png)   


![visualize2](https://github.com/mrtanush18/real-estate-data-pipeline/blob/main/3.png)  
