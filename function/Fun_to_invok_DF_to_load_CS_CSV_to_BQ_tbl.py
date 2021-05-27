#Cloud Function to run Data Flow Pipeline on new file upload in Google Storage functions-df-gcs-bq-load-job

#############main.py##################
def startDataflowProcess(data, context):
	from googleapiclient.discovery import build
	#replace with your projectID
	project = "third-campus-303308"
	job = project + " " + str(data['timeCreated'])
	#path of the dataflow template on google storage bucket
	template = "gs://third-campus-303308-df-bucket/sample-template/template_data_ingestion_df"
	inputFile = "gs://" + str(data['bucket']) + "/" + str(data['name'])
	bq_table_name = "ds_bigmart.big_mart_cf"
	#user defined parameters to pass to the dataflow pipeline job
	parameters = {
		'inputFile': inputFile, 'bq_table': bq_table_name
	}
	#tempLocation is the path on GCS to store temp files generated during the dataflow job
	environment = {'tempLocation': 'gs://third-campus-303308-df-bucket/temp-location'}

	service = build('dataflow', 'v1b3', cache_discovery=False)
	#below API is used when we want to pass the location of the dataflow job
	request = service.projects().locations().templates().launch(
		projectId=project,
		gcsPath=template,
		location='europe-west2',
		body={
			'jobName': job,
			'parameters': parameters,
			'environment':environment
		},
	)
	response = request.execute()
	print(str(response))

