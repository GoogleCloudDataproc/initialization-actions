import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassifier, XGBoostClassificationModel}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

val label = "1"
val schema = StructType(Array(
    StructField("c0", DoubleType),
    StructField("c1", DoubleType),
    StructField(label, IntegerType)))

val features = schema.filter(_.name != label).map(_.name).toArray

val datas = Seq(Row(1.05, 9.05, 0), 
               Row(2.95, 1.95, 1))
val df = spark.createDataFrame(
      spark.sparkContext.parallelize(datas),schema)

val commParamMap = Map(
  "tree_method" -> "gpu_hist",
  "objective" -> "binary:logistic",
  "num_workers" -> 1,
  "num_round" -> 100)

val xgbClassifier = new XGBoostClassifier(commParamMap)
      .setLabelCol(label)
      .setFeaturesCol(features)

xgbClassifier.fit(df)

classifier = XGBoostClassifier(**params).setLabelCol(label).setFeaturesCols(features)
model = classifier.fit(df)
