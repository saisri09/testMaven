package com.ca.ci.enrichment

import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
/**
  * Created by punma03 on 2/09/2017.
  */

/**
  * *
  *
  * This program reads will get the value of key from the conf file
  *
  */

object readEnrichmentPropFile {
  var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty
  var rulekey:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty
  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setAppName("").setMaster("local")
    val sparkConf = new SparkConf().setAppName("test")
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().appName("test").getOrCreate()
    //val sparkSession = SparkSession.builder.appName("StrikeironInfoEnrichment").config("spark.sql.warehouse.dir", "file:///C:/tempwarehouse").getOrCreate()


    //ruleMap = getval(sparkSession)
   // val keyval="sd"
    //val dbPwd = readEnrichmentPropFile.getpassval(sparkSession,"rwpass")
    //print("\n********inside****dbPwd******getpassval:"+dbPwd)
   // val value=getpassval(sparkSession,keyval)
    //print("dbPwd:"+dbPwd)

  }

 def getval(sparkSession: SparkSession,filePath:String): scala.collection.mutable.Map[String, String] =
  {               //wasb://staging@imsshdfsqa.blob.core.windows.net/sfdc/contact/
    //val filePath = "wasb://staging@imsshdfsqa.blob.core.windows.net/sfdc/contact/CASparkJobEnrichmentConfig.csv"
    val rules = sparkSession.read.option("header", "true").csv(filePath)
    rules.toDF().collect().foreach(x => ruleMap = ruleMap + (x.getString(0).toString -> x.getString(1).toString))
    return ruleMap
 }
  def getpassval(sparkSession: SparkSession,User:String,myConfigFile: String): String =
  {
    //print("\n********inside****readPropFile******getpassval:"+User)
    //val myConfigFile = "wasb://staging@imsshdfsqa.blob.core.windows.net/sfdc/contact/CASparkJobEnrichmentkey.csv"
    val rules = sparkSession.read.option("header", "true").csv(myConfigFile)
    rules.toDF().collect().foreach(x => rulekey = rulekey + (x.getString(0).toString -> x.getString(1).toString))
    val key=rulekey.get(User).get
    new String(Base64.decodeBase64(key.getBytes))
  }

 /*def getval(sparkSession: SparkSession): scala.collection.mutable.Map[String, String] =
  {
    var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty
    val bufferedSource = Source.fromFile("C://CA/job_resources/CASparkJobEnrichmentConfig.csv")
    //var ruleMap = scala.collection.mutable.Map[String, Rule]()
    for (line <- bufferedSource.getLines.drop(1))
    {
      val cols = line.split(",").map(_.trim)
      if(!cols.isEmpty && cols.size>1)
      //if(cols(0)!=null && cols(1)!=null)
      {
        ruleMap = ruleMap + (cols(0) -> cols(1))
      }
      //
    }
    bufferedSource.close
    println("ruleMap:**********"+ruleMap)
    return ruleMap
  }



  def getpassval(sparkSession: SparkSession,User:String): String =
  {
    var rulekey:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty
    val bufferedSource = Source.fromFile("C://CA/job_resources/CASparkJobEnrichmentkey.csv")
    println("bufferedSource:**********"+bufferedSource)
    for (line <- bufferedSource.getLines.drop(1))
    {
      val cols = line.split(",").map(_.trim)
      if(!cols.isEmpty && cols.size>1)
      //if(cols(0)!=null && cols(1)!=null)
      {
        rulekey = rulekey + (cols(0) -> cols(1))
      }
      //
    }
    println("rulekey:**********"+rulekey)
    val key=rulekey.get(User).get
    new String(Base64.decodeBase64(key.getBytes))
  }
*/
}