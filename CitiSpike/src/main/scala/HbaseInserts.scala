import HBaseAtomicity.{getHBaseConfiguration, getPosnSchema, getTranSchema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object HbaseInserts {

  def getHBaseConfiguration(): Configuration = {
    val conf = HBaseConfiguration.create()
    System.setProperty("user.name", "hdfs")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    conf.set("hbase.master", "127.0.0.1:60000")
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.setInt("hbase.zookeeper.property.clientPort", 2181)
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.setInt("hbase.client.scanner.caching", 10000)
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    return conf
  }

  def writeToHbase(line: Row, tableName: String): Unit = {
    var conf: Configuration = getHBaseConfiguration()
    conf.set("hbase.mapred.outputtable", tableName)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val table = new HTable(conf, tableName)
    val put: Put = new Put((line.get(0).toString + line.get(2).toString).getBytes)
    for (i <- 0 to line.length - 1) {
      put.addColumn("position".getBytes, line.schema.fieldNames(i).toString.getBytes, line.get(i).toString.getBytes)
    }

    table.put(put)
    table.flushCommits()
    table.close()

  }

  def getTranSchema(): StructType = {


    StructType(
      Array(
        StructField("ACCT_KEY", StringType),
        StructField("PP_CODE", StringType),
        StructField("TXN_TYPE", StringType),
        StructField("AMOUNT", StringType)
      )
    )

  }

  def getPosnSchema(): StructType = {

    StructType(
      Array(
        StructField("ACCT_KEY", StringType),
        StructField("PP_CODE", StringType),
        StructField("POSN", StringType),
        StructField("AMOUNT", StringType)
      )
    )

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.executor.memory", "8g").appName("Test").master("local[1]").getOrCreate()

    import spark.implicits._

    val txnDF = spark.read.option("header", "true").schema(getTranSchema()).csv("/Users/techops/txn.csv").as[TransactionSch]
    val posnDf = spark.read.option("header", "true").schema(getPosnSchema()).csv("/Users/techops/position.csv").as[Position]

    val partitionedTxn = txnDF.repartition($"ACCT_KEY")
    val partitionedPosn = posnDf.repartition($"ACCT_KEY")
    partitionedPosn.distinct().show()
    //partitionedPosn.select("*").where($"ACCT_KEY")

    val groupedData = partitionedPosn.groupBy($"ACCT_KEY")

    val groupsRdd = partitionedPosn.groupByKey(x => x.ACCT_KEY)

groupsRdd.mapGroups(x=> {

})
    //println(groupsRdd.AcctKey)
    //println("************"+groupsRdd.posn.AcctKey)



  }

}


case class TransactionSch(ACCT_KEY: String, PP_CODE: String, TXN_TYPE: String, AMOUNT: String)

case class Position(ACCT_KEY: String, PP_CODE: String, POSN: String, AMOUNT: String)

case class Group(AcctKey : String, posn:Position)
