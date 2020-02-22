from h2o.automl import H2OAutoML
from pyspark.sql import SparkSession
from pysparkling import *

spark = SparkSession.builder.appName("SparklingWaterApp").getOrCreate()
hc = H2OContext.getOrCreate(spark)

bucket = "h2o-bq-large-dataset"
train_path = "demos/cc_train.csv"
test_path = "demos/cc_test.csv"
y = "DEFAULT_PAYMENT_NEXT_MONTH"
is_classification = True

drop_cols = []
aml_args = {"max_runtime_secs": 120}

train_data = spark.read\
                  .options(header='true', inferSchema='true')\
                  .csv("gs://{}/{}".format(bucket, train_path))
test_data = spark.read\
                 .options(header='true', inferSchema='true')\
                 .csv("gs://{}/{}".format(bucket, test_path))

print("CREATING H2O FRAME")
training_frame = hc.as_h2o_frame(train_data)
test_frame = hc.as_h2o_frame(test_data)

x = training_frame.columns
x.remove(y)

for col in drop_cols:
    x.remove(col)

if is_classification:
    training_frame[y] = training_frame[y].asfactor()
else:
    print("REGRESSION: Not setting target column as factor")

print("TRAINING H2OAUTOML")
aml = H2OAutoML(**aml_args)
aml.train(x=x, y=y, training_frame=training_frame)

print(aml.leaderboard)

print('SUCCESS')
