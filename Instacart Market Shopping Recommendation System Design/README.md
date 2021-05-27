# Instacart Market Shopping Recommendation System Design

Whether you shop from meticulously planned grocery lists or let whimsy guide your grazing, our unique food rituals define who we are. Instacart, a grocery ordering and delivery app, aims to make it easy to fill your refrigerator and pantry with your personal favorites and staples when you need them. After selecting products through the Instacart app, personal shoppers review your order and do the in-store shopping and delivery for you.

Instacart’s data science team plays a big part in providing this delightful shopping experience. Currently they use transactional data to develop models that predict which products a user will buy again, try for the first time, or add to their cart next during a session. Recently, Instacart open sourced this data - see their blog post on [3 Million Instacart Orders, Open Sourced.](https://tech.instacart.com/3-million-instacart-orders-open-sourced-d40d29ead6f2)

Please refer to this URL for details about this competition:

https://www.kaggle.com/c/instacart-market-basket-analysis/overview.

## Data Description

The dataset for this competition is a relational set of files describing customers' orders over time. The goal of the competition is to predict which products will be in a user's next order. The dataset is anonymized and contains a sample of over 3 million grocery orders from more than 200,000 Instacart users. For each user, we provide between 4 and 100 of their orders, with the sequence of products purchased in each order. We also provide the week and hour of day the order was placed, and a relative measure of time between orders. For more information, see the [blog post](https://tech.instacart.com/3-million-instacart-orders-open-sourced-d40d29ead6f2) accompanying its public release.

Please refer to this URL for details about more details of data:

https://www.kaggle.com/c/instacart-market-basket-analysis/data

## Project Main Steps

1. Analysis souce data and create **ER Diargam** based on relationships between each entity.
2. Design and implement **Data Pipeline Archtecture** for processing this complex, large volume of datasets on **AWS Cloud Service**.
3. Design and implement **Machine Learning Model Architecture** for model training and prediction, deploying, and connecting to front-end web service.

### ER Diagram

![image-20210528010938787](https://tva1.sinaimg.cn/large/008i3skNgy1gqxe0grah0j30v20hogoa.jpg)

### Data Pipeline Archtecture

![image-20210528011014928](https://tva1.sinaimg.cn/large/008i3skNgy1gqxe10y1uaj31440o0n91.jpg)

**Process Flow**

1. Upload souce data to **S3 (Data Lake)**.
2. Used **Glue Crawler** to inject data into **Glue Catalog** for table formatting.
3. Transformed data and created features table by using **Athena** triggerd by Lambda functions.
4. Used **lambda** to triger **Glue job** to join features table into fact table then convert to spark dataframe, stored data into **S3**.
5. Orchestrated all **lambda functions** into **Step Functions** to achieve automated workflow.
6. Deployed a build-in **XGBoost model** for model training and predicting, and set up an endpoint on **SageMaker Service**.
7. Migrated the feature data of the model to the **DynamoDB** for optimizing data invocation.
8. Established an API to connect front-end web interface by using **API Gateway** triggerd by **Lambda function**.

### **Machine Learning Model Architecture** 

![image-20210528012549086](https://tva1.sinaimg.cn/large/008i3skNgy1gqxeh92k40j60us0lwqdu02.jpg)

Please refer to this URL for details about more details of **AWS SageMaker Service**:

https://docs.aws.amazon.com/sagemaker/index.html

## Project Application Result

**Web Page Display**

![image-20210528012958999](https://tva1.sinaimg.cn/large/008i3skNgy1gqxelk4w5rj30mo0b6tb3.jpg)

The user can enter customer id and product id on front-end web service to get the response about the likelihood of customers reordering a previously purchased product.

## Project Files

* `code` folder: contains python code of Glue Job and Lambda function. 

  (**Note: Here Lambda function use Athena for feature data invocation in step 7 on data pipeline part, not DynamoDB, DynamoDB implementation information can be found in [here](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html)**)

* `projection_instrution` folder: contains all project instructions files.
* `sagemaker` foler: contains instructions about deploying ML models on SageMaker and web service.html.

## Technologies Required:

* **AWS Cloud Service**: S3, Athena, Glue, Lambda function, Step Functions, SageMaker, API Gateway
* **Machine Learning**: XGBoost, feature engineering
* **Languages**: Python, SQL, Spark

## Future Works

* **Create a fully automated process** **for** **whole** **data** **pipeline** **and** **model** **training**.

  **flow chart**

  ![image-20210528021007539](https://tva1.sinaimg.cn/large/008i3skNgy1gqxfrbiu9aj30w808u43m.jpg)

