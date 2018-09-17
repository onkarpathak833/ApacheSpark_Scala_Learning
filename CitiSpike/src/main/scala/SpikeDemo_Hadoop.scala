import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.col
object SpikeDemo_Hadoop {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("HadoopAPI").master("local[1]").getOrCreate()

    import sparkSession.implicits._

    val accountSchema = StructType(
      Array(
        StructField("AccountNumber", StringType),
        StructField("TxnDate", StringType),
        StructField("TxnType", DoubleType),
        StructField("Amount", StringType)
      )
    )
    val seq = Seq("Dummy_ID","Date_of_Txn","Transaction_Type","Amount").map(col(_))


    val txnDf = sparkSession.read.option("header","true").csv("/Users/techops/Documents/Kafka/Sample_data.csv")
    txnDf.groupBy()


  }


}
