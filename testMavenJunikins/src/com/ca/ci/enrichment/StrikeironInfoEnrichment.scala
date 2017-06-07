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
object StrikeironInfoEnrichment {
  /*
  map for property file
   */
  var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty

  val appname = "StrikeironInfoEnrichment"
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

    //filePath="wasb://staging@imsshdfsqa.blob.core.windows.net/sfdc/contact/CASparkJobEnrichmentConfig.csv"
    //myConfigFile="wasb://staging@imsshdfsqa.blob.core.windows.net/sfdc/contact/CASparkJobEnrichmentkey.csv"

    /* val sparkConf = new SparkConf().setAppName(appname).setMaster("local")
     val sparkContext = new SparkContext(sparkConf)
     val sparkSession = SparkSession.builder.appName("StrikeironInfoEnrichment").config("spark.sql.warehouse.dir","file:///C:/tempwarehouse").getOrCreate()
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

    val instanceName="Strikeiron_Info_Enrichment_load_"+System.currentTimeMillis()
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

    val backDateDays=ruleMap.get("backDateDaysStrikeiron").get
    println("backDays:=" + backDateDays)

    val StrikeironUrl=ruleMap.get("StrikeironUrl").get
    val StrikeironHost=ruleMap.get("StrikeironHost").get
    val Strikeironcanonicaluri=ruleMap.get("Strikeironcanonicaluri").get

    val throttlingFactorStrikeiron =ruleMap.get("throttlingFactorStrikeiron").get
    val strikeironTimeout =ruleMap.get("strikeironTimeout").get

    println("strikeironTimeout:=" + strikeironTimeout)

    val timeToWaitInsecondStrikeiron = ruleMap.get("timeToWaitInsecondStrikeiron").get
    println("throttlingFactorStrikeiron:=" + throttlingFactorStrikeiron)

    val access_key=ruleMap.get("access_key").get
    val secret_key=ruleMap.get("secret_key").get
    try {
      /*
     Get the 15 days back date from current date
      */
      val cal:Calendar  = Calendar.getInstance();
      cal.add(Calendar.DATE, backDateDays.toInt)
      val laterdate=cal.getTime
      val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
      val backDate = dateFormatter.format(laterdate)
      println("backDate:"+backDate)
      /*
      get all the records from v_batch_run_details
       */
      val dbBatchRunDetails = sparkSession.read.jdbc(jdbcURL, "mdm.daily_strikeiron_vw", connectionProperties).select("person_email_key")
      println("dbBatchRunDetails:"+dbBatchRunDetails.show())
      /*
       load person table
      */
      //val dbPerson = sparkSession.read.jdbc(jdbcURL, "mdm.person", connectionProperties).select("person_key", "source_system_person_id", "first_name", "last_name")

      /*
      join batch run view and person table
       */
      //val PersonRunDetailDF=dbBatchRunDetails.join(dbPerson,"source_system_person_id")
      /*
      load email table
       */
      val dbEmail = sparkSession.read.jdbc(jdbcURL, "mdm.person_email", connectionProperties).select("person_key", "email_address","person_email_key","updated_date")

      /*
      join PersonRunDetailDF with email table to get email address
       */
      val filterBatchRunView=dbBatchRunDetails.join(dbEmail,"person_email_key")

      val now = Calendar.getInstance().getTime()
      val currentDate = dateFormatter.format(now)
      /*
      take only current update records from person email
      */
      filterBatchRunView.createOrReplaceTempView("filterBatchRunViewEmail")
      val dbPersonRunfilter = sparkSession.sql("select * from filterBatchRunViewEmail where updated_date like'" + currentDate + "%'")
      println("Current date Updated records count:"+dbPersonRunfilter.show())
      /*
       load person_email_verification_api_history to remove 15 days back records from person email
       */
      val personEmailVerificationApi = sparkSession.read.jdbc(jdbcURL, "mdm.person_email_verification_api_history", connectionProperties).select("person_email_key","updated_date")
      personEmailVerificationApi.createOrReplaceTempView("personEmailVerificationApiStrikeiron")
      val dbEmailverification = sparkSession.sql("select person_email_key from personEmailVerificationApiStrikeiron where updated_date  >='"+backDate+"'")
      //print("\nrecords to be filter out"+dbEmailverification.show())


      val dbfilterBatchRunViewPersonKey = dbPersonRunfilter.select("person_email_key")
      val dbEmailFilter = dbfilterBatchRunViewPersonKey.except(dbEmailverification)
      print("\nrecords after filter"+dbEmailFilter.show())


      val dbEmailFinalRecords = dbPersonRunfilter.join(dbEmailFilter, "person_email_key")
      //print("\ndbEmailFinalRecords"+dbEmailFinalRecords.show())
//
      val filterEmail=dbEmailFinalRecords.filter(dbEmailFinalRecords("email_address").rlike("\\A(?=[a-z0-9@.!#$%&'*+\\/=?^_`{|}~-]{6,254}\\z)(?=[a-z0-9.!#$%&'*+\\/=?^_`{|}~-]{1,64}@)[a-z0-9!#$%&'*+\\/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+\\/=?^_`{|}~-]+)*@(?:(?=[a-z0-9-]{1,63}\\.)[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+(?=[a-z0-9-']{1,63}\\z)[a-z0-9'](?:[a-z0-9-']*[a-z0-9'])?\\z"))

      filterEmail.createOrReplaceTempView("strikeironEmailInfoTempTable")
      val filteredEmaiDF=sparkSession.sql("select distinct email_address from strikeironEmailInfoTempTable")

      val junkEmailpattern = sparkSession.read.jdbc(jdbcURL, "mdm.person_email_junk_pattern", connectionProperties).select("email_address")

      val finalEmailDFtoFire=filteredEmaiDF.except(junkEmailpattern)
      println("Number of records to fire"+finalEmailDFtoFire.count())
       /*
      call AWSRequestSigner method strikeironApiEmailVerification for each email address
       */
      val elementList = finalEmailDFtoFire.toDF().collect()
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
        emailsrecordList.createOrReplaceTempView("receivedEmailsRecordListFromApiCall")
        val emailDF = sparkSession.sql("select distinct a.*,b.person_key,b.person_email_key,current_timestamp() as email_verification_date from receivedEmailsRecordListFromApiCall a,strikeironEmailInfoTempTable b where a.email_address=b.email_address")
        val dropEmailDF = emailDF.drop("email_address")
        dropEmailDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.person_email_verification_api_history_temp", connectionProperties)
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
    StrikeironApi(row.getAs("email_address"),com.ca.ci.enrichment.AWSRequestSigner.strikeironApiEmailVerification(access_key,secret_key,StrikeironUrl,StrikeironHost,Strikeironcanonicaluri,row.getAs("email_address"),strikeironTimeout))
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
