# Fun-to-invoke-DF-to-load-CSV-to-BQ
Cloud function triggered when a CSV file placed into the triggered landing storage bucket path and invoke the dataflow to load CSV data into BigQuery table 

# Steps to load CSV to BQ table

### Create two buckets. 
1-One for landing i.e. trigger bucket for function i.e. third-campus-303308-cf-landing

2-Another for Dataflow to store the temporary files during processing i.e. third-campus-303308-df-bucket

### Create dataflow template using content in DataFlow folder
1- Put data_ingestion.py & requirements.txt files in the cloud shell 

2- Execute Create_DF_Template_Code.txt scripts in cloud shell to create dataflow template

3- Check in the gs://third-campus-303308-df-bucket/sample-template to verify the dataflow (template_data_ingestion_df) created or not

### Create cloud function using content in function folder
1- Create function with name 'functions-df-gcs-bq-load-job' with below details

  -Function name : functions-df-gcs-bq-load-job
  
  -Region : europe-west2     --the region should be same for storage, function and dataflow to avoid the failure

  -Trigger type : Cloud storage
  
  -Event type : Finalize/Create
  
  -bucket : third-campus-303308-cf-landing    --i.e. triggered landing bucket
  
  -Source : edit the main.py and requirements.txt 
      i.e. copy the code from Fun_to_invok_DF_to_load_CS_CSV_to_BQ_tbl.py into the main.py 
      and  copy the code from requirements.txt to requirements.txt
      
  -Runtime : Python 3.7
  
  -Entry point : startDataflowProcess   i.e. function name of the main.py code
  
### Place the csv file into the  third-campus-303308-cf-landing  bucket to tigger the function and dataflow to load data into bigquery table.
  
  
  
