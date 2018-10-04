import HBaseAtomicity.{getHBaseConfiguration, getPosnSchema, getTranSchema}
import HbaseInserts.{getHBaseConfiguration, getPosnSchema, getTranSchema}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.shell.CopyCommands
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object SparkHbase_SHC {

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
    conf.set("hbase.mapred.outputtable","Positions")
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


  def positionCatalog: String =
    s"""{
       |"table":{"namespace":"default", "name":"PositionData"},
       |"rowkey":"ACCT_KEY",
       |"columns":{
       |"ACCT_KEY":{"cf":"rowkey", "col":"ACCT_KEY", "type":"string"},
       |"POSN":{"cf":"position", "col":"POSN", "type":"string"},
       |"PP_CODE":{"cf":"position", "col":"PP_CODE", "type":"string"},
       |"AMOUNT":{"cf":"position", "col":"AMOUNT", "type":"string"}
       |}
       |}""".stripMargin



  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().config("spark.executor.memory", "8g").appName("Test").master("local[1]").getOrCreate()

    import spark.implicits._
    LogManager.getRootLogger.setLevel(Level.ERROR)
    val txnDF = spark.read.option("header", "true").schema(getTranSchema()).csv("/Users/techops/txn.csv").as[TransactionSch]
    val posnDf = spark.read.option("header", "true").schema(getPosnSchema()).csv("/Users/techops/position.csv").as[Position]

    val partitionedTxn = txnDF.repartition($"ACCT_KEY")

    val partitionedPosn = posnDf.repartition($"ACCT_KEY")

    val partitionedRDD: RDD[Position] = partitionedPosn.rdd
    val groupedData = partitionedPosn.rdd.groupBy(line => line.ACCT_KEY)
    val keys: Array[String] = groupedData.keys.distinct().collect()


    println("Start Time : "+System.currentTimeMillis())

    val groupPartitions = partitionedPosn.groupByKey(data => data.ACCT_KEY)

    val groupKeys = groupPartitions.keys.collect()
    groupKeys.foreach(data => {

      val filteredDs: Dataset[Position] = partitionedPosn.filter(x => x.ACCT_KEY==data)

      filteredDs.write.options(Map(HBaseTableCatalog.tableCatalog -> positionCatalog,HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()
      println("DF pushed for key : "+data)

      val statusTable = new HTable(getHBaseConfiguration(),"Status")

      val put = new Put(data.getBytes)
      put.addColumn("Position_Status".getBytes,"ACCT_KEY".getBytes,data.getBytes)
      statusTable.put(put)
      statusTable.flushCommits()

    })

  }


}
