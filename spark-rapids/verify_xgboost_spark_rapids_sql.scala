import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

val data1 = Seq(
  Row(1, "John Doe"),
  Row(2, "Jane Smith"),
  Row(3, "Bob Johnson")
)

val schema1 = StructType(
  List(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true)
  )
)

val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data1), schema1)
df1.createOrReplaceTempView("table1")

val data2 = Seq(
  Row(1, "Engineer"),
  Row(2, "Doctor"),
  Row(3, "Scientist"),
  Row(4, "Artist")
)

val schema2 = StructType(
  List(
    StructField("id", IntegerType, true),
    StructField("occupation", StringType, true)
  )
)

val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema2)
df2.createOrReplaceTempView("table2")

val joinedDF = spark.sql(s"SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.id")

joinedDF.show()

spark.sql(s"INSERT INTO table1 VALUES (4, 'Alice Wonderland')")

val resultDF = spark.sql(s"SELECT * FROM table1")

resultDF.show()
