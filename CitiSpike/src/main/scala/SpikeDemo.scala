import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.{JobConf, TextOutputFormat}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.HTable

object SpikeDemo {

  def printMonthlyBalanceData(sc:SparkContext):Unit={

    val txnRdd = sc.textFile("/Users/techops/Documents/Kafka/Sample_data.csv", 1)

    val acctKeyGrp = txnRdd.groupBy { x => x.split(",")(0)+x.split(",")(1).substring(2, 5) }

    //acctKeyGrp.saveAsTextFile("/Users/techops/Documents/Kafka/output3.txt")
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val groupDataframe = acctKeyGrp.toDF()

    acctKeyGrp.foreach(x => {
      val accountNumber = x._1.substring(0,x._1.length()-3)
      val txnMonth = x._1.substring(x._1.length()-3,x._1.length())
      print("For Account Number = "+accountNumber+" Txn Month = "+txnMonth+"\n")
      var creditAmt = 0
      var debitAmt = 0
      var totalBalance = 0
      x._2.foreach { x =>

        var accNum = x.split(",")(0)
        var txnDate = x.split(",")(1)
        val txnType = x.split(",")(2)
        val amount = x.split(",")(3)
        if(txnType.equals("c")){
          creditAmt = creditAmt + amount.toInt
        }
        if(txnType.equals("D")){
          debitAmt = debitAmt + amount.toInt
        }
        print("Txn Type = "+txnType+" Txn Date = "+txnDate+" Amount = "+amount+"\n")
      }
      totalBalance = debitAmt - creditAmt
      print("Total Balance for Month "+txnMonth+" = "+totalBalance+"\n")
    })

  }


  def checkDebitCreditData(txnRdd:RDD[String]):Unit={


    val groupRdd = txnRdd.map { x => (x.split(",")(0),x) }.groupByKey()
    groupRdd.foreach(x => {
      val accountNumber = x._1
      var debitAmount = 0
      var creditAmount = 0
      //print("Processing for account no. - "+accountNumber+" with details - ")
      x._2.foreach { x => {
        //print(x+"\n")
        var allData = x.split(",")
        var txnType = allData(2)
        var amount = allData(3)
        //print(txnType+" of "+amount+"\n")
        if(txnType.equals("D")){
          debitAmount = debitAmount.toInt + amount.toInt
        }
        if(txnType.equals("C")){
          creditAmount = creditAmount.toInt + amount.toInt
        }
      }

        //print(accountNumber+" Debit Amt. - "+debitAmount+" Credit Amt. - "+creditAmount+"\n")

      }
      var totalBalance = debitAmount - creditAmount
      //println("Account Number = "+accountNumber+"  Total Balance = "+totalBalance)
    }

    )


  }

  def connectToHBase():JobConf={

    val conf = HBaseConfiguration.create()
      val outputTable = "txn_data"
    import org.apache.hadoop.hbase.client.HTable
    val myTable = new HTable(conf, outputTable)
         System.setProperty("user.name", "hdfs")
         System.setProperty("HADOOP_USER_NAME", "hdfs")
          conf.set("hbase.master", "127.0.0.1:60000")
          conf.set("hbase.mapred.outputtable", outputTable)
          conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
          conf.setInt("timeout", 120000)
          conf.set("hbase.zookeeper.quorum", "localhost")
    conf.setInt("hbase.zookeeper.property.clientPort",  2181)
          conf.set("zookeeper.znode.parent", "/hbase-unsecure")
          conf.setInt("hbase.client.scanner.caching", 10000)
    conf.set("zookeeper.znode.parent","/hbase-unsecure")
          //.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Result]))
          val jobConfig: JobConf = new JobConf(conf,this.getClass)
         jobConfig.setOutputFormat(classOf[TableOutputFormat])
         jobConfig.set(TableOutputFormat.OUTPUT_TABLE,outputTable)

          return jobConfig
  }


  def main(args: Array[String]): Unit = {


    val sc = new SparkContext(new SparkConf().setAppName("test-data").setMaster("local[1]"))

    val txnRdd = sc.textFile("/Users/techops/Documents/Kafka/Sample_data.csv", 1)

    val testHadoopRdd = sc.textFile("/Users/techops/Documents/Kafka/Sample_data.csv", 1)

    //val testHadoopRdd = sc.newAPIHadoopFile("/Users/techops/Documents/Kafka/Sample_data.csv")

    //val txnRdd = sc.newAPIHadoopFile("/Users/techops/Documents/Kafka/Sample_data.csv")
    //printMonthlyBalanceData(sc)

    /*
    *
    * In line def details
    *
    * */

    //val txnRdd = sc.textFile("/Users/techops/Documents/Kafka/Sample_data.csv", 1)

    val acctKeyGrp = txnRdd.groupBy { x => x.split(",")(0)+x.split(",")(1).substring(2, 5) }

    //acctKeyGrp.saveAsTextFile("/Users/techops/Documents/Kafka/output3.txt")
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //val groupDataframe = acctKeyGrp.toDF()
    val testDs = testHadoopRdd.toDS()
    val monthlyRdd = sc.emptyRDD


    val jobConfig = connectToHBase()
    val dataList:List[String] = List()

    acctKeyGrp.foreach(x => {

      val accountNumber = x._1.substring(0,x._1.length()-3)
      val txnMonth = x._1.substring(x._1.length()-3,x._1.length())
      print("For Account Number = "+accountNumber+" Txn Month = "+txnMonth+"\n")
      var creditAmt = 0
      var debitAmt = 0
      var totalBalance = 0
      var dataMap = new mutable.HashMap[String,String]()

      val finalRDD = x._2.foreach { x =>

        var accNum = x.split(",")(0)
        var txnDate = x.split(",")(1)
        val txnType = x.split(",")(2)
        val amount = x.split(",")(3)
        if(txnType.equals("c")){
          creditAmt = creditAmt + amount.toInt
        }
        if(txnType.equals("D")){
          debitAmt = debitAmt + amount.toInt
        }
        print("Txn Type = "+txnType+" Txn Date = "+txnDate+" Amount = "+amount+"\n")
      }
      totalBalance = debitAmt - creditAmt
      print("Total Balance for Month "+txnMonth+" = "+totalBalance+"\n")
      //val hadoopRdd = sc.newAPIHadoopFile(accountNumber+","+txnMonth+","+totalBalance)
      //hadoopRdd.saveAsNewAPIHadoopDataset(jobConfig)




    })

    val sortedRdd = testHadoopRdd.sortBy(x => x.split(",")(0)+x.split(",")(1))

    val allData = sortedRdd.map(x => {

      var index = 1;
      index = index + 1
      var txndata = x.split(",")

      val kv: KeyValue = new KeyValue((System.currentTimeMillis()+x.split(",")(0)+x.split(",")(1)).getBytes, "txn".getBytes(), "all_data".getBytes,x.getBytes)
      (new ImmutableBytesWritable(Bytes.toBytes(System.currentTimeMillis()+""+index)),kv)


    })


    allData.saveAsNewAPIHadoopFile("/Users/techops/Documents/Hbase/Data/Data2", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], jobConfig)
    //allData.saveAsTextFile("/Users/techops/Documents/Spark")
    //allData.saveAsNewAPIHadoopFile("/Users/techops/Documents", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], jobConfig)
    val bulkLoader = new LoadIncrementalHFiles(jobConfig)
    val table = new HTable(jobConfig,"txn_data")

    bulkLoader.doBulkLoad(new Path("/Users/techops/Documents/Hbase/Data/Data2"), table)
  }


}
