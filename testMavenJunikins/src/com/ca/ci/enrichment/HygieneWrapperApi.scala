package com.ca.ci.enrichment

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by punma03 on 3/9/2017.
  */
object HygieneWrapperApi {
  /*
  case class and StandardizationPhoneList to store the Standardized phone number from google lib
   */
  case class PhoneDetails(telephone_number:String,standardised_phone_number:String,is_valid_flag:String,country_code:String,person_phone_key:String)

  var phoneStandardizationList:scala.collection.mutable.ListBuffer[PhoneDetails]=scala.collection.mutable.ListBuffer.empty
  /*
  map for config file
   */
  var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty
  /*
  sparkjob constant
   */
  val appname = "HygienePhoneEnrichment"
  val file_key = 1
  val started = "started"
  val ended = "ended"
  var filePath:String=null
  var myConfigFile:String=null
   /*
  main spark job main invocation
   */
  def main(arg: Array[String]) = {
    val sparkConf = new SparkConf().setAppName(appname)
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().appName(appname).getOrCreate()

  /*val sparkConf = new SparkConf().setAppName(appname).setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder.appName("HygienePhoneEnrichment").config("spark.sql.warehouse.dir","file:///C:/tempwarehouse").getOrCreate()
    */

    filePath=arg(0)
    myConfigFile=arg(1)

    //filePath="wasb://staging@imsshdfsqa.blob.core.windows.net/sfdc/contact/CASparkJobEnrichmentConfig.csv"
    //myConfigFile="wasb://staging@imsshdfsqa.blob.core.windows.net/sfdc/contact/CASparkJobEnrichmentkey.csv"
    ruleMap = readEnrichmentPropFile.getval(sparkSession,filePath)
    val jdbcURL=ruleMap.get("jdbcURL").get
    val dbUser=ruleMap.get("dbUser").get
    val userName=ruleMap.get("userName").get
    val dbPwd = readEnrichmentPropFile.getpassval(sparkSession,"rwpass",myConfigFile)

    val connectionProperties = new Properties()
    connectionProperties.put("user", dbUser)
    connectionProperties.put("password", dbPwd)
    /*
     getting data from spark_jobs_master
    */
    val jobsMaster = sparkSession.read.jdbc(jdbcURL, "mdm.spark_jobs_master", connectionProperties).select("batch_id")
    jobsMaster.createOrReplaceTempView("jobs_master_table")
    //print(jobsMaster.show())
    val maxJobKey=sparkSession.sql("select max(batch_id) as job_id from jobs_master_table").head()get(0)

    val entitydetail = sparkSession.read.jdbc(jdbcURL, "mdm.entity_detail", connectionProperties).select("entity_detail_key").where(" entity_name='Hygiene_Phone_Enrichment' ")
    // print("entitydetailkey"+entitydetailkey.collect().toList)

    val entity_detail_key=entitydetail.collect().toList(0).get(0).toString
   // print("entity_detail_key: "+entity_detail_key)

    val file_key=1
    val instanceName="Hygiene_Phone_Melissa_ServiceObject_"+System.currentTimeMillis()

    val jobStatusStart = sparkSession.sql("""select distinct """+maxJobKey+""" as batch_id,'"""+instanceName+"""' as instance_name,'started' as status,'"""+file_key+"""' as file_key ,
   '"""+entity_detail_key+"""' as entity_detail_key ,current_timestamp() as created_date,'"""+userName+"""' as created_user
    from jobs_master_table a """)
    //print(jobStatusStart.show())
    jobStatusStart.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)

    processRecords(sparkSession,ruleMap)

    /*
     saving spark job start and end time in  spark_jobs_run_details
    */
    val jobStatusEnd = sparkSession.sql("select distinct "+maxJobKey+" as batch_id,'"+instanceName+"'"+" as instance_name,'ended' as status,'"+file_key+"' as file_key , '"+entity_detail_key+"' as entity_detail_key ,current_timestamp() as created_date,'"+userName+"' as created_user from jobs_master_table")
    jobStatusEnd.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)
    val endTime = System.currentTimeMillis()

  }
  def processRecords(sparkSession: SparkSession,ruleMap:scala.collection.mutable.Map[String, String]): Unit = {
    /*
    reading config files value
     */
    val jdbcURL=ruleMap.get("jdbcURL").get
    val dbUser=ruleMap.get("dbUser").get
    val userName=ruleMap.get("userName").get
    val dbPwd = readEnrichmentPropFile.getpassval(sparkSession,"rwpass",myConfigFile)

    val melissaurl=ruleMap.get("melissaurl").get
    val melissahost=ruleMap.get("melissahost").get
    val melissacanonicaluri=ruleMap.get("melissacanonicaluri").get

    val access_key=ruleMap.get("access_key").get
    val secret_key=ruleMap.get("secret_key").get

    val throttlingFactormelissa =ruleMap.get("throttlingFactormelissa").get
    val timeToWaitInsecondmelissa = ruleMap.get("timeToWaitInsecondmelissa").get

    val backDateDaysMelissa=ruleMap.get("backDateDaysMelissa").get
    val backDateDaysServiceObject=ruleMap.get("backDateDaysServiceObject").get

    val serviceObjectUrl=ruleMap.get("serviceObjectUrl").get
    val serviceObjecthost=ruleMap.get("serviceObjecthost").get
    val serviceObjectcanonicaluri=ruleMap.get("serviceObjectcanonicaluri").get

    val throttlingFactorserviceObject =ruleMap.get("throttlingFactorserviceObject").get
    val timeToWaitInsecondserviceObject = ruleMap.get("timeToWaitInsecondserviceObject").get

    val connectionProperties = new Properties()
    connectionProperties.put("user", dbUser)
    connectionProperties.put("password", dbPwd)
    println("throttlingFactormelissa: "+throttlingFactormelissa)
    println("throttlingFactorserviceObject: "+throttlingFactorserviceObject)
    /*
    load batch run view
     */
    val dbBatchRunDetails = sparkSession.read.jdbc(jdbcURL, "mdm.v_batch_run_details", connectionProperties)
    /*
    load person tabel
     */
    //val dbPerson = sparkSession.read.jdbc(jdbcURL, "mdm.person", connectionProperties).select("person_key", "source_system_person_id", "first_name", "last_name")
    /*
    load person phone
     */
    val dbEnrichmentApiPersonPhone = sparkSession.read.jdbc(jdbcURL, "mdm.person_phone", connectionProperties).select("person_key","telephone_number","person_phone_key","updated_date").where("telephone_number is not null")
    /*
    join batch view and person table
     */
   // val PersonRunDetailDF=dbBatchRunDetails.join(dbPerson,"source_system_person_id")

    /*
    PersonRunDetailDF and person_phone table
     */
    val dbPersonPhoneAndPerson=dbBatchRunDetails.join(dbEnrichmentApiPersonPhone,"person_key")

    /*
    get current date
     */
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    val now = Calendar.getInstance().getTime()
    val currentDate = dateFormatter.format(now)

    /*
    filter records with current date from dbPersonPhoneAndPerson data frame
     */
    dbPersonPhoneAndPerson.createOrReplaceTempView("dbPersonPhoneAndPersonTempView")
    val dbPersonRunfilter = sparkSession.sql("select * from dbPersonPhoneAndPersonTempView where updated_date like'" + currentDate + "%'")
    //print("dbPersonRunfilter"+dbPersonRunfilter.show())
    println("person_phone table current updated records count:NEW "+dbPersonRunfilter.count())

    /*
    load bridge_person_address address  country_synonym table to get country code
     */
    val dbbridgepersonaddress = sparkSession.read.jdbc(jdbcURL, "mdm.bridge_person_address", connectionProperties).select("address_key","person_key")
    val dbaddress = sparkSession.read.jdbc(jdbcURL, "mdm.address", connectionProperties).select("address_key","country")
    val countrysynonym = sparkSession.read.jdbc(jdbcURL, "mdm.country_synonym", connectionProperties).select("country_synonym","country_code")
    val countrysynonymCountry = countrysynonym.select(countrysynonym("country_synonym").alias("country"),countrysynonym("country_code"))

    /*
    join above table to get country code
     */
    val dbEnrichPersonPhone=dbPersonRunfilter.join(dbbridgepersonaddress,"person_key")
    val dbBridgeAddress=dbEnrichPersonPhone.join(dbaddress,"address_key")
    val dbPersonPhoneCountry=dbBridgeAddress.join(countrysynonymCountry,"country")
    //print("dbPersonPhoneCountry"+dbPersonPhoneCountry.show())
    //println("dbPersonPhoneCountry"+dbPersonPhoneCountry.count())

     /*
    send the data frame of person table to hygienephoneEnrichment to filter the records
     */
    print("filteredBackdatedRecords count"+dbPersonPhoneCountry.show())
    /*
    validate the phone number with google Library
    */
    //print("phoneDetails"+filteredBackdatedRecords.count())
    dbPersonPhoneCountry.toDF().collect().foreach(x => phoneValidation(x.getAs("telephone_number"),x.getAs("person_phone_key").toString,x.getAs("country_code")))

    import sparkSession.implicits._
    val phoneDetails=phoneStandardizationList.toDF()
    print("phoneDetails count"+phoneDetails.show())

    val verification_reason_key=50000001

    val filteredBackdatedRecords=HygienePhoneEnrichment.phoneEnrichment(sparkSession,phoneDetails,backDateDaysMelissa,jdbcURL,connectionProperties)
    filteredBackdatedRecords.createOrReplaceTempView("StandardizationPhoneListMelissaHyginene")


    val googleStandardizationHistory=sparkSession.sql("select person_phone_key,standardised_phone_number,is_valid_flag,'"+verification_reason_key+"' as verification_reason_key,telephone_number,current_timestamp() as created_date,'"+userName+"' as created_user_id,current_timestamp() as updated_date,'"+userName+"' as updated_user_id from StandardizationPhoneListMelissaHyginene")
    //print("googlestandardizationhistory"+googleStandardizationHistory.show())

    /*
    store the list in person_phone_google_standardiration_history
    */
    googleStandardizationHistory.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.person_phone_google_standardization_history", connectionProperties)
    /*
    Fire the Melissa aws api for filter records
    */
    //print("phoneDetails"+phoneDetails.show())
    MelissaDataVerification.processMelissaDataRecords(sparkSession,filteredBackdatedRecords,access_key,secret_key,melissaurl,melissahost,melissacanonicaluri,throttlingFactormelissa,timeToWaitInsecondmelissa,jdbcURL,connectionProperties)
    /*
    get the records from temp table for service object
     */
    val enrichmentPhoneVerificationApiHistorytemp = sparkSession.read.jdbc(jdbcURL, "mdm.person_phone_verification_api_history_temp", connectionProperties)
    //print("enrichmentphoneverificationtemp"+enrichmentPhoneVerificationApiHistorytemp.show())

    /*
    filter records for Ps02 and ps16 write the meaning of code
     */
    //val filterrecordsforServiceobject=enrichmentPhoneVerificationApiHistorytemp.filter(!enrichmentPhoneVerificationApiHistorytemp("Results").like("PS02") or !enrichmentPhoneVerificationApiHistorytemp("Results").like("PS16"))
    val filterrecordsforServiceobject=enrichmentPhoneVerificationApiHistorytemp.filter(enrichmentPhoneVerificationApiHistorytemp("Results").contains("PS02") or enrichmentPhoneVerificationApiHistorytemp("Results").contains("PS16"))

    //print("filterrecordsforServiceobject"+filterrecordsforServiceobject.show())

     /*
     Fire the Service object api for filter records
     */
    if(filterrecordsforServiceobject.count() > 0) {
      ServiceObjectsVerification.processServiceObjectsRecords(filterrecordsforServiceobject, sparkSession, access_key, secret_key, serviceObjectUrl, serviceObjecthost, serviceObjectcanonicaluri, throttlingFactorserviceObject, timeToWaitInsecondserviceObject, jdbcURL, connectionProperties,backDateDaysServiceObject)
    }else
      {
        println("Message : No records to process for serviceproject")
      }
  }
  /*
  method will call Phonevalidationgoogle object method
   */
  def phoneValidation(telephone_number:String,person_phone_key:String,country_code:String): Unit =
  {
    //print("\ntelephone_number"+telephone_number+" person_phone_key:"+person_phone_key+" country_code:"+country_code)
    //print("\ntelephone_number"+telephone_number)
    val response = PhoneValidationGoogle.googlePhoneValidation(telephone_number,person_phone_key,country_code)
    if(response!=null) {
      phoneStandardizationList.append(PhoneDetails(response.telephone_number, response.standardised_phone_number.toString, response.isValid.toString, response.country_code, response.person_phone_key))
    }
  }

}
