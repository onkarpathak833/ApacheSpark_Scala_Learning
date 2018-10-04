import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Spike_SHC {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("spark-hbase-connector").master("local[1]").getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("spark.hbase.host","localhost")
    import spark.implicits._

    val txnSchema = StructType(
      Array(
        StructField("AccountNumber", StringType),
        StructField("Date", StringType),
        StructField("TxnType", StringType),
        StructField("Amount", StringType)
      )
    )

    val monthlyBalanceSchema = StructType(
      Array(
        StructField("AccountNumber", StringType),
        StructField("TxnMonth", StringType),
        StructField("TxnYear", StringType),
        StructField("TotalBalance", StringType)
      )
    )
    import spark.implicits._

    val txnData = spark.read.option("header",true).schema(txnSchema).csv("/Users/techops/Documents/Kafka/Sample_data.csv")
      .as[Transaction1]
txnData.schema(0)
    txnData.repartition($"AccountNumber").foreachPartition( p=> {

    })
    val ds = txnData.mapPartitions(x => x.map(data =>

    {
  (data.Amount,data.AccountNumber,data.TxnType,data.Date.substring(2,5),data.Date.substring(5,9))

    })
    ).groupBy("_4","_5","_2")

    ds.count().show()

//    txnData.mapPartitions(lines => lines.map(x=>{
//
//      val acctNum = x.AccountNumber
//      val txnType = x.Amount
//      val txnDate = x.Date.substring(2,x.Date.length)
//
//    })).as[Transaction]





  }
}

case class MonthTransaction(AccountNumber: String, TxnMonth: String, TxnYear: String, TotalBalance: String)

case class Transaction1(AccountNumber: String, Date: String, TxnType: String, Amount: String)