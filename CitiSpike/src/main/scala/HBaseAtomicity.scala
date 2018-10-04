import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.jruby.compiler.ir.Tuple

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

object HBaseAtomicity {


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

  def writeListToHbase(list:List[IndexedSeq[(ImmutableBytesWritable,KeyValue)]]): Unit ={

    val spark = SparkSession.builder().getOrCreate()
    val rdd = spark.sparkContext.parallelize(list)

  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.executor.memory", "8g").appName("Test").master("local[1]").getOrCreate()

    import spark.implicits._

    val txnDF = spark.read.option("header", "true").schema(getTranSchema()).csv("/Users/techops/txn.csv").as[Transaction]
    val posnDf = spark.read.option("header", "true").schema(getPosnSchema()).csv("/Users/techops/position.csv").as[Position]

    val partitionedTxn = txnDF.repartition($"ACCT_KEY")
    val partitionedPosn = posnDf.repartition($"ACCT_KEY")
    //val groupAcctKey = partitionedPosn.rdd.groupBy(line => {line.get(0)})
    val groupAcctKey = partitionedPosn.rdd.groupBy(line => line.ACCT_KEY)

//  val groupedData = partitionedPosn.groupBy($"ACCT_KEY")
//
//  val groupsRdd = partitionedPosn.groupByKey(x => x.AcctKey)
//
//    groupsRdd.mapValues(data => {
//     // data.get(0)
//      println(data)
//    })

//    val keyVelues = groupAcctKey.map(f = data => {
//
//       data._2.map(x=>{
//
//        val rowKey = x.get(0).toString
//
//                for (i <- 0 to x.size - 1) yield {
//                  val kv: KeyValue = new KeyValue((rowKey.toString).getBytes, "position".getBytes, x.schema(0).toString().getBytes, x.get(0).toString.getBytes)
//                  (new ImmutableBytesWritable(rowKey.toString.getBytes), kv)
//                }
//      })
//
//    })



    val keyVelues = groupAcctKey.map(f = data => {

      data._2.flatMap(x => {

        val rowKey = x.ACCT_KEY

        for (i <- 0 to 3) yield {
          val kv: KeyValue = new KeyValue((rowKey.toString).getBytes, "position".getBytes, x.ACCT_KEY.toString().getBytes, x.ACCT_KEY.toString.getBytes)
          (new ImmutableBytesWritable(rowKey.toString.getBytes), kv)
        }
      }).toList


      val vlist: RDD[(ImmutableBytesWritable, KeyValue)] = spark.sparkContext.parallelize(List())
      val configuration = getHBaseConfiguration()
      configuration.set("hbase.mapred.outputtable", "Positions")
      vlist.saveAsNewAPIHadoopDataset(configuration)

    })




    //    rdds.foreach(rdd => {
    //
    //      val dataRdd = rdd.map(data => {
    //        val rowKey = data.get(0)
    //
    //        for (i <- 0 to data.size - 1) {
    //          val kv: KeyValue = new KeyValue((rowKey.toString).getBytes, "position".getBytes, data.schema(i).toString().getBytes, data.get(i).toString.getBytes)
    //          (new ImmutableBytesWritable((rowKey.toString).getBytes), kv)
    //        }
    //
    //      })
    //      println(rdd)


    //    })
    //      val dataRdd1 = groupAcctKey.map(data => {
    //
    //        val rowKey = data._2.iterator.next().get(0)
    //        for (i <- 0 to data._2.size - 1) {
    //
    //          val kv: KeyValue = new KeyValue((rowKey.toString).getBytes, "position".getBytes, x.schema(i).toString().getBytes, x.get(i).toString.getBytes)
    //
    //          (new ImmutableBytesWritable((rowKey.toString).getBytes), kv)
    //
    //
    //        }

    //})

//    val perAcctData = groupAcctKey.foreach(data => {
//
//      val accRdd = data._2.map(x => {
//
//        val rowKey = x.get(0) + x.get(2).toString
//
//        for (i <- 0 to x.length - 1) {
//          val kv: KeyValue = new KeyValue((rowKey.toString).getBytes, "position".getBytes, x.schema(i).toString().getBytes, x.get(i).toString.getBytes)
//
//          (new ImmutableBytesWritable((rowKey.toString).getBytes), kv)
//
//        }
//
//      })
//
//
//    })


    var index = 0
//    groupAcctKey.foreach(data => {
//
//      data._2.foreach(row => {
//        println(row.get(0) + "," + row.get(1) + "," + row.get(2) + "," + row.get(3))
//        writeToHbase(row, "Positions")
//        index = index + 1
//      })

  //  })
    println("After Insert size is : " + index)


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
}

case class Transaction(AcctKey: String, PpCode: String, TxnType: String, Amount: String)

//case class Position(AcctKey: String, PpCode: String, Posn: String, Amount: String)



