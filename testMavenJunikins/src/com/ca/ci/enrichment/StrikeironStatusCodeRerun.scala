package com.ca.ci.enrichment

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by punma03 on 3/6/2017.
  */
object StrikeironStatusCodeRerun {
  /*
  map for property file
   */
  var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty

  val appname = "StrikeironStatusCodeRerun"
  val file_key = 1
  val started = "started"
  val ended = "ended"
  val instanceName = appname + System.currentTimeMillis()
  /*
  case class to store the email records
   */
  case class EmailDetails(email_address:String,
                          StatusNbr:String,
                          StatusDescription:String,
                          Code:String,
                          Description:String,
                          HygieneResult:String,
                          NetProtected:String,
                          NetProtectedBy:String,
                          SourceIdentifier:String,
                          Email:String,
                          LocalPart:String,
                          DomainPart:String,
                          IronStandardCertifiedTimestamp:String,
                          DomainKnowledge:String,
                          StringKeyValuePairKey:String,
                          StringKeyValuePairValue:String

                         )
  /*
  buffer emaillist to store records comming from aws api call
   */
  var emailList:scala.collection.mutable.ListBuffer[EmailDetails]=scala.collection.mutable.ListBuffer.empty
  var filePath:String=null
  var myConfigFile:String=null
  /*
  main method will invoke first
  */
  def main(arg: Array[String]) = {

    val sparkConf = new SparkConf().setAppName(appname)
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().appName(appname).getOrCreate()

    filePath=arg(0)
    myConfigFile=arg(1)

    //filePath="wasb://staging@imsshdfsqa.blob.core.windows.net/person_master_config/hygiene_enrichment/CASparkJobEnrichmentConfig.csv"
    //myConfigFile="wasb://staging@imsshdfsqa.blob.core.windows.net/person_master_config/hygiene_enrichment/CASparkJobEnrichmentkey.csv"


    /*
     val sparkConf = new SparkConf().setAppName(appname).setMaster("local")
     val sparkContext = new SparkContext(sparkConf)
     val sparkSession = SparkSession.builder.appName("StrikeironStatusCodeRerun").config("spark.sql.warehouse.dir","file:///C:/tempwarehouse").getOrCreate()
    */
    ruleMap = readEnrichmentPropFile.getval(sparkSession,filePath)
    val jdbcURL=ruleMap.get("jdbcURL").get
    val dbUser=ruleMap.get("dbUser").get
    val userName=ruleMap.get("userName").get
    val dbPwd = readEnrichmentPropFile.getpassval(sparkSession,"rwpass",myConfigFile)
    val connectionProperties = new Properties()
    connectionProperties.put("user", dbUser)
    connectionProperties.put("password", dbPwd)
    //print("ruleMap"+ruleMap)
    /*
     getting data from spark_jobs_mastera
    */
    val jobsMaster = sparkSession.read.jdbc(jdbcURL, "mdm.spark_jobs_master", connectionProperties).select("batch_id")
    jobsMaster.createOrReplaceTempView("jobs_master_table")
    //print(jobsMaster.show())
    val maxJobKey=sparkSession.sql("select max(batch_id) as job_id from jobs_master_table").head()get(0)

    //person_email_verification_api_history_temp

    val entitydetail = sparkSession.read.jdbc(jdbcURL, "mdm.entity_detail", connectionProperties).select("entity_detail_key").where(" entity_name='person_email_verification_api_history_temp' ")
    val entity_detail_key=entitydetail.collect().toList(0).get(0).toString
    //println("entity_detail_key:=" + entity_detail_key)
    val file_key=1

    val instanceName="Strikeiron_StatusCode_Rerun_Load"+System.currentTimeMillis()

    val jobStatusStart = sparkSession.sql("""select distinct """+maxJobKey+""" as batch_id,'"""+instanceName+"""' as instance_name,'started' as status,'"""+file_key+"""' as file_key ,
   '"""+entity_detail_key+"""' as entity_detail_key ,current_timestamp() as created_date,'"""+userName+"""' as created_user
    from jobs_master_table a """)
    //print(jobStatusStart.show())
    jobStatusStart.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)
    /*
    main method to process records
    */
    processRecords(sparkSession,ruleMap,jdbcURL,connectionProperties,userName)

    /*
    saving spark job start and end time in  spark_jobs_run_details
    */
    val jobStatusEnd = sparkSession.sql("select distinct "+maxJobKey+" as batch_id,'"+instanceName+"'"+" as instance_name,'ended' as status,'"+file_key+"' as file_key , '"+entity_detail_key+"' as entity_detail_key ,current_timestamp() as created_date,'"+userName+"' as created_user from jobs_master_table")
    jobStatusEnd.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)
    //val endTime = System.currentTimeMillis()

    sparkSession.stop()
    sparkContext.stop()

  }
  /*
  mehod will accept the spark session and process the reords
   */
  def processRecords(sparkSession: SparkSession,ruleMap:scala.collection.mutable.Map[String, String],jdbcURL:String,connectionProperties:Properties,userName:String): Unit = {


    val StrikeironUrl=ruleMap.get("StrikeironUrl").get
    val StrikeironHost=ruleMap.get("StrikeironHost").get
    val Strikeironcanonicaluri=ruleMap.get("Strikeironcanonicaluri").get

    val throttlingFactorStrikeiron =ruleMap.get("throttlingFactorStrikeiron").get
    val strikeironTimeout =ruleMap.get("strikeironTimeout").get

    println("strikeironTimeout:=" + strikeironTimeout)

    val timeToWaitInsecondStrikeiron = ruleMap.get("timeToWaitInsecondStrikeiron").get
    //println("throttlingFactorStrikeiron:=" + throttlingFactorStrikeiron)

    val access_key=ruleMap.get("access_key").get
    val secret_key=ruleMap.get("secret_key").get
    try {

      /*
      get all the records from person_email_verification_api_history_temp
       */
      val dbReunDetails = sparkSession.read.jdbc(jdbcURL, "mdm.person_email_verification_api_history_temp", connectionProperties).where("statusNbr='220' and Code='222'")

      println("number of records to process:"+dbReunDetails.count())

      dbReunDetails.createOrReplaceTempView("StrikeironStatusCodeRerunTempView")

      val filteredEmaiDF=sparkSession.sql("select distinct Email from StrikeironStatusCodeRerunTempView")
      println("number of records to process after removing duplicates:"+filteredEmaiDF.count())
      val elementList = filteredEmaiDF.toDF().collect()

      invokeAPIWithThrottling(elementList,access_key,secret_key,StrikeironUrl,StrikeironHost,Strikeironcanonicaluri,throttlingFactorStrikeiron,timeToWaitInsecondStrikeiron,strikeironTimeout)

    }catch
      {
        case e: Exception => {
          print("ERROR:Exception in processRecords"+e.toString()+e.printStackTrace())
        }
      }finally
    {
      /*
      save the records in enrichment_email_temp by converting the emailsrecordList in data frame
      */
      import sparkSession.implicits._
      if(!emailList.isEmpty) {
        val emailsrecordList=emailList.toDF()
        emailsrecordList.createOrReplaceTempView("receivedEmailsRerunRecordListFromApiCall")
        val emailDF = sparkSession.sql("select distinct a.*,b.person_key,b.person_email_key,current_timestamp() as email_verification_date from receivedEmailsRerunRecordListFromApiCall a,StrikeironStatusCodeRerunTempView b where a.email_address=b.Email")
        val dropEmailDF = emailDF.drop("email_address")
        dropEmailDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.person_email_verification_api_history_temp_rerun", connectionProperties)
      }
    }
  }

  /*
  Main Throttling method will invoke api as per Throttling factor
   */
  def invokeAPIWithThrottling(dataArray:Array[org.apache.spark.sql.Row],access_key:String,secret_key:String,StrikeironUrl:String,StrikeironHost:String,Strikeironcanonicaluri:String,throttlingFactorStrikeiron:String,timeToWaitInsecondStrikeiron:String,strikeironTimeout:String)=
  {
    import scala.collection.parallel._
    val dataList = dataArray.toList
    if(!dataList.isEmpty)
    {
      val parallelList =dataList.par
      parallelList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(throttlingFactorStrikeiron.toInt))
      parallelList.map(x=>invokeAPI(x,access_key,secret_key,StrikeironUrl,StrikeironHost,Strikeironcanonicaluri,strikeironTimeout))
    }
  }

  /*
  invoke AWSSigner method
   */
  def invokeAPI(row:org.apache.spark.sql.Row,access_key:String,secret_key:String,StrikeironUrl:String,StrikeironHost:String,Strikeironcanonicaluri:String,strikeironTimeout:String)
  {
    StrikeironApi(row.getAs("Email"),com.ca.ci.enrichment.AWSRequestSigner.strikeironApiEmailVerification(access_key,secret_key,StrikeironUrl,StrikeironHost,Strikeironcanonicaluri,row.getAs("Email"),strikeironTimeout))
  }

  /*
  method will get the responce from aws api and process all the fields
 */
  def StrikeironApi(email_address:String,response:String): Unit = {
    try{
      if (response != null) {
        /*
      parsing the json in tree format to get the result
       */
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        val resultEmail = mapper.readTree(response)

        var StatusNbr: String = null
        var StatusDescription: String = null
        var Code: String = null
        var Description: String = null
        var HygieneResult: String = null
        var NetProtected: String = null
        var NetProtectedBy: String = null
        var SourceIdentifier: String = null
        var Email: String = null
        var LocalPart: String = null
        var DomainPart: String = null
        var IronStandardCertifiedTimestamp:String = null
        var DomainKnowledge: String = null
        var StringKeyValuePairKey: String = null
        var StringKeyValuePairValue: String = null
        /*
      get the fields and store in the emailList
       */
        if (resultEmail.has("WebServiceResponse")) {
          if (resultEmail.get("WebServiceResponse").has("VerifyEmailResponse")) {
            if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").has("VerifyEmailResult")) {

              if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").has("ServiceStatus")) {

                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceStatus").has("StatusNbr")) {
                  StatusNbr = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceStatus").get("StatusNbr").asText()
                }
                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceStatus").has("StatusDescription")) {
                  StatusDescription = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceStatus").get("StatusDescription").asText()
                }
              }

              if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").has("ServiceResult")) {

                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").has("Reason")) {
                  if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("Reason").has("Code")) {
                    Code = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("Reason").get("Code").asText()
                  }
                  if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("Reason").has("Description")) {
                    Description = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("Reason").get("Description").asText()
                  }
                }

                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").has("HygieneResult")) {
                  HygieneResult = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("HygieneResult").asText()
                }

                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").has("NetProtected")) {
                  NetProtected = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("NetProtected").asText()
                }
                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").has("NetProtectedBy")) {
                  NetProtectedBy = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("NetProtectedBy").asText()
                }
                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").has("SourceIdentifier")) {
                  if(!resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("SourceIdentifier").asText().equalsIgnoreCase("null")) {
                    SourceIdentifier = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("SourceIdentifier").asText()
                  }
                }
                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").has("Email")) {
                  Email = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("Email").asText()
                }
                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").has("LocalPart")) {
                  LocalPart = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("LocalPart").asText()
                }
                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").has("DomainPart")) {
                  DomainPart = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("DomainPart").asText()
                }
                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").has("IronStandardCertifiedTimestamp")) {
                  if(!resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("IronStandardCertifiedTimestamp").asText().equalsIgnoreCase("")) {
                    IronStandardCertifiedTimestamp = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("IronStandardCertifiedTimestamp").asText()
                  }
                }
                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").has("DomainKnowledge")) {
                  if(!resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("DomainKnowledge").asText().equalsIgnoreCase("null")) {
                    DomainKnowledge = resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("DomainKnowledge").asText()
                  }
                }

                if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").has("AddressKnowledge")) {
                  if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("AddressKnowledge").has("StringKeyValuePair")) {
                    if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("AddressKnowledge").get("StringKeyValuePair").has("Key")) {
                      StringKeyValuePairKey=resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("AddressKnowledge").get("StringKeyValuePair").get("Key").asText()
                    }
                    if (resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("AddressKnowledge").get("StringKeyValuePair").has("Value")) {
                      StringKeyValuePairValue=resultEmail.get("WebServiceResponse").get("VerifyEmailResponse").get("VerifyEmailResult").get("ServiceResult").get("AddressKnowledge").get("StringKeyValuePair").get("Value").asText()
                    }
                  }
                }
              }

              emailList.append(EmailDetails(
                email_address,
                StatusNbr,
                StatusDescription,
                Code,
                Description,
                HygieneResult,
                NetProtected,
                NetProtectedBy,
                SourceIdentifier,
                Email,
                LocalPart,
                DomainPart,
                IronStandardCertifiedTimestamp,
                DomainKnowledge,
                StringKeyValuePairKey,
                StringKeyValuePairValue))
            }
          }
        }
      }
    }catch {
      case e: Exception => {
        println("ERROR :Exception in StrikeironApi method email_address:"+email_address+"\nresponse:"+response+ e.toString()+ e.printStackTrace())

      }
    }
  }
}
