# We need to use the low-level library to interact with SageMaker since the SageMaker API
# is not available natively through Lambda.
import boto3
import time
import csv

#define function to get predict model input features
def get_input(user_id,product_id):
    athena_client = boto3.client('athena')
    s3_resource = boto3.resource('s3')

    # set athena variables
    database = 'set your database here'
    query_output = 'set your query output bucket here'

    # set query
    query1 = '''
    select * from test20220429
    where user_id = {var_1}
    and product_id = {var_2}
    '''.format(var_1=user_id,var_2=product_id)

    # execution query by client
    query_resp = athena_client.start_query_execution(
        QueryString=query1,
        QueryExecutionContext={ 'Database': database },
        ResultConfiguration={ 'OutputLocation': query_output}
    )
    
    time.sleep(5)

    # get the query execution id
    execution_id = query_resp['QueryExecutionId']
    print('the execution id is: '+ execution_id)
    
    results = athena_client.get_query_execution(
        QueryExecutionId=execution_id
        )
    result_location = results['QueryExecution']['ResultConfiguration']['OutputLocation']
    print('LOCATION IS: '+ result_location)

    ## set s3 features
    s3_feature = result_location.split('//')[1]
    bucket = s3_feature.split('/',1)[0]
    file_key = s3_feature.split('/',1)[1]

    # get data
    s3_object = s3_resource.Object(bucket, file_key)
    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()

    rows = []
    lines = csv.reader(data)
    for line in lines:
        rows.append(line)
    test_input = rows[1][3:]
    sagemaker_input = ' '.join(test_input).replace(' ',',')

    return sagemaker_input
################################

def lambda_handler(event, context):

    body = event['body']
    feature = event['body']
    user_id, product_id = feature.split(',')
    
    model_input = get_input(user_id,product_id)
    

    # The SageMaker runtime is what allows us to invoke the endpoint that we've created.
    runtime = boto3.Session().client('sagemaker-runtime')

    # Now we use the SageMaker runtime to invoke our endpoint, sending the review we were given
    response = runtime.invoke_endpoint(EndpointName = 'set your sagemaker-xgboost-endpoint here',# The name of the endpoint we created
                                       ContentType = 'text/csv',                 # The data format that is expected
                                       Body = model_input
                                       )

    # The response is an HTTP response whose body contains the result of our inference
    result = response['Body'].read().decode('utf-8')

    # Round the result so that our web app only gets '1' or '0' as a response.
    result = float(result)

    return {
        'statusCode' : 200,
        'headers' : { 'Content-Type' : 'text/plain', 'Access-Control-Allow-Origin' : '*' },
        'body' : str(result)
    }