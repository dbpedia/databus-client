package sparkTests

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec

class sparkTest extends AnyFlatSpec{


//  "Apache Spark" should "convert rdd to dataFrame" in {
//    val spark: SparkSession =
//      SparkSession
//        .builder()
//        .appName("AppName")
//        .config("spark.master", "local")
//        .getOrCreate()
//
//    import spark.implicits._
//
//    def dfSchema(columnNames: List[String]): StructType =
//      StructType(
//        Seq(
//          StructField(name = "name", dataType = StringType, nullable = false),
//          StructField(name = "age", dataType = IntegerType, nullable = false)
//        )
//      )
//
//    def row(line: List[String]): Row = Row(line(0), line(1).toInt)
//
//    val simpleData = Seq("Fabi","23","Jakob", "26")
//
//
//    val rdd: RDD[String] = spark.sparkContext.parallelize(simpleData)
//
//    val dataFrame2 = rdd.toDF()
//    dataFrame2.show(false)
//    val schema = dfSchema(List("name", "age"))
//    val data = rdd.map(_.split(",").map(row)
//    val dataFrame = spark.createDataFrame(data, schema)
//
//
//    dataFrame.show(false)
//  }

  "Apache Spark" should "create DataFrame" in {
//    case class StructType(fields: Array[StructField])
//    case class StructField(name: String, dataType: DataType)

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local")
        .getOrCreate()

    val simpleData = Seq(Row("James","Smith","36636","M"), Row("Fabi", "G", "1", "M"))

    val simpleSchema = StructType(Array(
      StructField("firstname",StringType),
      StructField("lastname",StringType),
      StructField("id", StringType),
      StructField("gender", StringType)
    ))

    println(simpleSchema.getClass)

    val rdd: RDD[Row] = spark.sparkContext.parallelize(simpleData)

    val dataFrame = spark.createDataFrame(rdd, simpleSchema)

    dataFrame.show(false)
  }

  "Apache Spark" should "also create DataFrame" in {
//    case class OwnStructType(firstname:String,lastname:String, id: String, gender:String)
//
//    val spark: SparkSession =
//      SparkSession
//        .builder()
//        .appName("AppName")
//        .config("spark.master", "local")
//        .getOrCreate()
//
//    val simpleData = Seq(Row("James","Smith","36636","M"), Row("Fabi", "G", "1", "M"))
//  val simpleSchema = ScalaReflection.schemaFor[OwnStructType].dataType.asInstanceOf[StructType]
//
//    println(simpleSchema.getClass)
//
//    val rdd: RDD[Row] = spark.sparkContext.parallelize(simpleData)
//
//    val dataFrame = spark.createDataFrame(rdd, simpleSchema)
//
//    dataFrame.show(false)
  }
}
