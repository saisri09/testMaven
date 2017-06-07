package com.ca.ci.enrichment

import java.util.{Calendar, Properties}
import java.text.SimpleDateFormat
import java.util.Properties
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Spark Job for Melissa Data Reverse Lookup
  */
object MelissaDataReverseLookUp
{
  case class googlePhoneDetails(telephone_number:String,standardised_phone_number:String,is_valid_flag:String,country:String,person_phone_key:String)

  var googleStandardizationList:scala.collection.mutable.ListBuffer[googlePhoneDetails]=scala.collection.mutable.ListBuffer.empty
  //Case class for storage of result into mdm.enrichment_api_melissadata_reverse_phone_temp table
  case class MelissaReverseLookupRecord(standardised_phone_number:String,
                                        AddressExtras:String,
                                        AddressKey:String,
                                        AddressLine1:String,
                                        AddressLine2:String,
                                        City:String,
                                        CompanyName:String,
                                        EmailAddress:String,
                                        MelissaAddressKey:String,
                                        NameFull:String,
                                        PhoneNumber:String,
                                        PostalCode:String,
                                        RecordExtras:String,
                                        RecordID:String,
                                        Reserved:String,
                                        Results:String,
                                        State:String)
  //Case class for storage of result into mdm.enrichment_api_melissadata_reverse_phone_error_details table
  case class ErrorDetailsRecord( phone_number:String,error_message:String)

  var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty

  val appname="MelissaReverseLookupRecord"

  var PhoneList:scala.collection.mutable.ListBuffer[MelissaReverseLookupRecord]=scala.collection.mutable.ListBuffer.empty
  var filePath:String=null
  var myConfigFile:String=null
  /*
  main method
   */
  def main(arg: Array[String]): Unit =
  {

    val sparkConf = new SparkConf().setAppName(appname)
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().appName(appname).getOrCreate()

    filePath=arg(0)
    myConfigFile=arg(1)
    /*val sparkConf = new SparkConf().setAppName(appname).setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder.appName("MelissaReverseLookupRecord").config("spark.sql.warehouse.dir","file:///C:/tempwarehouse").getOrCreate()
    */
   /*
   getting config file properties
   */
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

    val file_key=1
    val instanceName="contact_hub_load"+System.currentTimeMillis()

    val entitydetail = sparkSession.read.jdbc(jdbcURL, "mdm.entity_detail", connectionProperties).select("entity_detail_key").where(" entity_name='clearbit_domain_verification_temp' ")
    //val entity_detail_key=entitydetail.rdd.map(x=>x(0).toString).collect().toList
    val entity_detail_key=entitydetail.collect().toList(0).get(0).toString

    val jobStatusStart = sparkSession.sql("""select distinct """+maxJobKey+""" as batch_id,'"""+instanceName+"""' as instance_name,'started' as status,'"""+file_key+"""' as file_key ,
   '"""+entity_detail_key+"""' as entity_detail_key ,current_timestamp() as created_date,'"""+userName+"""' as created_user
    from jobs_master_table a """)
    print(jobStatusStart.show())
    jobStatusStart.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)

    /*
    main processing method
     */
    processRecords(sparkSession,ruleMap,connectionProperties,jdbcURL)

    /*
    saving spark job start and end time in  spark_jobs_run_details
    */
    val jobStatusEnd = sparkSession.sql("select distinct "+maxJobKey+" as batch_id,'"+instanceName+"'"+" as instance_name,'ended' as status,'"+file_key+"' as file_key , '"+entity_detail_key+"' as entity_detail_key ,current_timestamp() as created_date,'"+userName+"' as created_user from jobs_master_table")
    jobStatusEnd.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)

  }

  /*
  main processing method
   */
  def processRecords(sparkSession:SparkSession,ruleMap:scala.collection.mutable.Map[String, String],connectionProperties:Properties,jdbcURL:String): Unit = {

    //print("ruleMap:" + ruleMap)
    //val jdbcURL=ruleMap.get("jdbcURL").get
    //val dbUser=ruleMap.get("dbUser").get
    val userName=ruleMap.get("userName").get
    //val dbPwd = readEnrichmentPropFile.getpassval(sparkSession,"rwpass",myConfigFile)
    val MelissaReverseUrl=ruleMap.get("MelissaReverseUrl").get
    val MelissaReversehost=ruleMap.get("MelissaReversehost").get
    val MelissaReversecanonicaluri=ruleMap.get("MelissaReversecanonicaluri").get

    val access_key=ruleMap.get("access_key").get
    val secret_key=ruleMap.get("secret_key").get

    val throttlingFactorMelissaReverse =ruleMap.get("throttlingFactorMelissaReverse").get
    println("throttlingFactorMelissaReverse:" + throttlingFactorMelissaReverse)
    val timeToWaitInsecondMelissaReverse = ruleMap.get("timeToWaitInsecondMelissaReverse").get
    //val connectionProperties = new Properties()
    //connectionProperties.put("user", dbUser)
    //connectionProperties.put("password", dbPwd)
    val backDateDays=ruleMap.get("backDateDaysMelissaReverse").get

    try {
    /*
      Get the 15 days back date from current date
     */
    val cal: Calendar = Calendar.getInstance();
    cal.add(Calendar.DATE, backDateDays.toInt)
    val laterdate = cal.getTime
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    val backDate = dateFormatter.format(laterdate)

    val dbBatchRunDetails = sparkSession.read.jdbc(jdbcURL, "mdm.v_batch_run_details", connectionProperties)
    val dbPersonPhone = sparkSession.read.jdbc(jdbcURL, "mdm.person_phone", connectionProperties).select("person_phone_key", "telephone_number","person_key")//.where("person_key in (606732,558912,535914,550900,463955,458338,457453,452415,441254,415033) ")
    val dbenrichmentapimelissadatareversephone = sparkSession.read.jdbc(jdbcURL, "mdm.enrichment_api_melissadata_reverse_phone", connectionProperties).select("person_phone_key", "phone_number","updated_date")
    //print("dbPersonPhone" + dbPersonPhone.show())
    /*
    make backdate for all spark job in conf file
    */
    val dbPersonPhoneAndPerson=dbBatchRunDetails.join(dbPersonPhone,"person_key")

    dbenrichmentapimelissadatareversephone.createOrReplaceTempView("dbenrichmentapimelissadatareversephone")
    val dbmelissadatareversephone = sparkSession.sql("select person_phone_key from dbenrichmentapimelissadatareversephone where updated_date  >='" + backDate + "'")
    //print("back dated records to be filter" + dbmelissadatareversephone.show())

    val filterRecord = dbPersonPhoneAndPerson.select("person_phone_key")
    //print("filterRecord" + filterRecord.show())

    val finalresult = filterRecord.except(dbmelissadatareversephone)
    //print("back dated records filtered" + finalresult.show())

    val fireDF=dbPersonPhoneAndPerson.join(finalresult,"person_phone_key")
    //print("fireDF" + fireDF.show())

    fireDF.toDF().collect().foreach(x => phoneValidation(x.getAs("telephone_number"),x.getAs("person_phone_key").toString,"US"))

    import sparkSession.implicits._
    val googleStandardizationDF=googleStandardizationList.toDF()
    //print("date frame before removing duplicates "+googleStandardizationDF.show())
    //print("googleStandardizationDF count"+googleStandardizationDF.count())

    googleStandardizationDF.createOrReplaceTempView("googleStandardizationMellisaReverseLookUp")

    val filteredDuplicateDF=sparkSession.sql("select distinct standardised_phone_number from googleStandardizationMellisaReverseLookUp")
    //print("data frame after removing duplicates"+filteredDuplicateDF.show())
    val finalresultList = filteredDuplicateDF.toDF().collect()


    invokeAPIWithThrottling(finalresultList.take(10),access_key,secret_key,MelissaReverseUrl,MelissaReversehost,MelissaReversecanonicaluri,throttlingFactorMelissaReverse,timeToWaitInsecondMelissaReverse)

    //print("backDate:" + backDate)
    }catch
      {
        case e: Exception => {
          print("ERROR: Exception :"+e.toString())
          e.printStackTrace(); e.toString()
        }
      }
    finally {
      /*
    finally store the data in temp table
     */
      import sparkSession.implicits._
      val PhoneListfinal = PhoneList.toDF()
      PhoneListfinal.createOrReplaceTempView("PhoneListfinalMelissaDataReverse")

      val phoneToMelissaReverseTempDF = sparkSession.sql("select distinct a.*,b.person_phone_key,current_timestamp() as phone_verification_date from PhoneListfinalMelissaDataReverse a,googleStandardizationMellisaReverseLookUp b where a.standardised_phone_number = b.standardised_phone_number")
      //println("phoneToMelissaReverseTempDF" + phoneToMelissaReverseTempDF.show())

      val dropColumnDFFinal=phoneToMelissaReverseTempDF.drop("standardised_phone_number")

      dropColumnDFFinal.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.enrichment_api_melissadata_reverse_phone_temp", connectionProperties)

      //val melissaFireDF=sparkSession.sql("select * from googleStandardizationMellisaReverse")
      //print("melissaFireDF count"+melissaFireDF.show())
      val verification_reason_key=50000001
      /*
      naming change for variable
       */
      val googleStandardizationHistory=sparkSession.sql("select person_phone_key,standardised_phone_number,is_valid_flag,'"+verification_reason_key+"' as verification_reason_key,telephone_number,current_timestamp() as created_date,'"+userName+"' as created_user_id,current_timestamp() as updated_date,'"+userName+"' as updated_user_id from googleStandardizationMellisaReverseLookUp")
      //print("googlestandardizationhistory"+googleStandardizationHistory.show())
      /*
      store the list in person_phone_google_standardiration_history
       */
      googleStandardizationHistory.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.person_phone_google_standardization_history", connectionProperties)
    }
  }

  def phoneValidation(telephone_number:String,person_phone_key:String,country:String): Unit =
  {
    //println("*****inside phoneValidation***1")
    val response = PhoneValidationGoogle.googlePhoneValidation(telephone_number,person_phone_key,country)
    if(response!=null) {
      //println("*****inside phoneValidation***2")
      googleStandardizationList.append(googlePhoneDetails(response.telephone_number, response.standardised_phone_number.toString, response.isValid.toString, response.country_code, response.person_phone_key))
    }
    //println("responce: googleStandardizationList:" + googleStandardizationList)
    // case class PhoneDetails(telephone_number:String,standardised_phone_number:String,is_valid_flag:String,country:String,person_phone_key:String)
  }
  /*
throttling algo mehod
*/
  def invokeAPIWithThrottling(dataArray:Array[org.apache.spark.sql.Row],access_key:String,secret_key:String,MelissaReverseUrl:String,MelissaReversehost:String,MelissaReversecanonicaluri:String,throttlingFactorMelissaReverse:String,timeToWaitInsecondMelissaReverse:String)=
  {
    // val pc = dataArray.par
    // println("throttlingFactor" + throttlingFactorStrikeiron.toInt)
    //val startTime = System.currentTimeMillis()
    //val timeToWaitInMillisecond = timeToWaitInsecondStrikeiron.toInt*1000
    import scala.collection.parallel._
    val dataList = dataArray.toList
    if(!dataList.isEmpty)
    {
      val parallelList =dataList.par
      parallelList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(throttlingFactorMelissaReverse.toInt))
      parallelList.map(x=>invokeAPI(x,access_key,secret_key,MelissaReverseUrl,MelissaReversehost,MelissaReversecanonicaluri))
      // parallelList.
    }
   // val endTime = System.currentTimeMillis()
   // println("Overall Time taken to process all records :"+(endTime-startTime )/1000)
  }

  /*
   invokeAPI method will call the aws api
    */
  def invokeAPI(row:org.apache.spark.sql.Row,access_key:String,secret_key:String,MelissaReverseUrl:String,MelissaReversehost:String,MelissaReversecanonicaluri:String)
  {
    //println("processing row number :"+row)
    createRecordFromJSONResponseObject(row.getAs("standardised_phone_number").toString,com.ca.ci.enrichment.AWSRequestSigner.melissaPhoneVerificationReverse(access_key,secret_key,MelissaReverseUrl,MelissaReversehost,MelissaReversecanonicaluri,row.getAs("standardised_phone_number")))//"847-264-5802"))//row.getAs("telephone_number")))
    //countDownLatch.countDown()
  }
    /**
    * This method would parse the JSON response and create a record
    *
    * @return
    */
  def createRecordFromJSONResponseObject(standardised_phone_number:String,response:String) =
  {
    try{
    if(response!=null) {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val resultPhone = mapper.readTree(response)
      var AddressExtras: String = null
      var AddressKey: String = null
      var AddressLine1: String = null
      var AddressLine2: String = null
      var City: String = null
      var CompanyName: String = null
      var EmailAddress: String = null
      var MelissaAddressKey: String = null
      var NameFull: String = null
      var PhoneNumber: String = null
      var PostalCode: String = null
      var RecordExtras: String = null
      var RecordID: String = null
      var Reserved: String = null
      var Results: String = null
      var State: String = null
      if(resultPhone.has("Records")) {
        if(resultPhone.get("Records").has(0)) {

          if(resultPhone.get("Records").get(0).has("AddressExtras")) {
            if(!resultPhone.get("Records").get(0).get("AddressExtras").asText().equalsIgnoreCase("null")) {
              AddressExtras = resultPhone.get("Records").get(0).get("AddressExtras").asText()
            }
          }
          if(resultPhone.get("Records").get(0).has("AddressKey")) {
            if(!resultPhone.get("Records").get(0).get("AddressKey").asText().equalsIgnoreCase("null")) {
              AddressKey = resultPhone.get("Records").get(0).get("AddressKey").asText()
            }
          }
          if(resultPhone.get("Records").get(0).has("AddressLine1")) {
            if(!resultPhone.get("Records").get(0).get("AddressLine1").asText().equalsIgnoreCase("null")) {
              AddressLine1 = resultPhone.get("Records").get(0).get("AddressLine1").asText()
            }
          }
          if(resultPhone.get("Records").get(0).has("AddressLine2")) {
            if(!resultPhone.get("Records").get(0).get("AddressLine2").asText().equalsIgnoreCase("null")) {
              AddressLine2 = resultPhone.get("Records").get(0).get("AddressLine2").asText()
            }
          }
          if(resultPhone.get("Records").get(0).has("City")) {
            if(!resultPhone.get("Records").get(0).get("City").textValue().equalsIgnoreCase("null")) {
              City = resultPhone.get("Records").get(0).get("City").textValue()
            }
          }
          if(resultPhone.get("Records").get(0).has("CompanyName")) {
            if(!resultPhone.get("Records").get(0).get("CompanyName").asText().equalsIgnoreCase("null")) {
              CompanyName = resultPhone.get("Records").get(0).get("CompanyName").asText()
            }
          }
          if(resultPhone.get("Records").get(0).has("EmailAddress")) {
            if(!resultPhone.get("Records").get(0).get("EmailAddress").textValue().equalsIgnoreCase("null")) {
              EmailAddress = resultPhone.get("Records").get(0).get("EmailAddress").textValue()
            }
          }
          if(resultPhone.get("Records").get(0).has("MelissaAddressKey")) {
            if(!resultPhone.get("Records").get(0).get("MelissaAddressKey").textValue().equalsIgnoreCase("null")) {
              MelissaAddressKey = resultPhone.get("Records").get(0).get("MelissaAddressKey").textValue()
            }
          }
          if(resultPhone.get("Records").get(0).has("NameFull")) {
            if(!resultPhone.get("Records").get(0).get("NameFull").textValue().equalsIgnoreCase("null")) {
              NameFull = resultPhone.get("Records").get(0).get("NameFull").textValue()
            }
          }
          if(resultPhone.get("Records").get(0).has("PhoneNumber")) {
            if(!resultPhone.get("Records").get(0).get("PhoneNumber").textValue().equalsIgnoreCase("null")) {
              PhoneNumber = resultPhone.get("Records").get(0).get("PhoneNumber").textValue()
            }
          }
          if(resultPhone.get("Records").get(0).has("PostalCode")) {
            if(!resultPhone.get("Records").get(0).get("PostalCode").textValue().equalsIgnoreCase("null")) {
              PostalCode = resultPhone.get("Records").get(0).get("PostalCode").textValue()
            }
          }
          if(resultPhone.get("Records").get(0).has("RecordExtras")) {
            if(!resultPhone.get("Records").get(0).get("RecordExtras").textValue().equalsIgnoreCase("null")) {
              RecordExtras = resultPhone.get("Records").get(0).get("RecordExtras").textValue()
            }
          }
          if(resultPhone.get("Records").get(0).has("RecordID")) {
            if(!resultPhone.get("Records").get(0).get("RecordID").textValue().equalsIgnoreCase("null")) {
              RecordID = resultPhone.get("Records").get(0).get("RecordID").textValue()
            }
          }
          if(resultPhone.get("Records").get(0).has("Reserved")) {
            if(!resultPhone.get("Records").get(0).get("Reserved").textValue().equalsIgnoreCase("null")) {
              Reserved = resultPhone.get("Records").get(0).get("Reserved").textValue()
            }
          }
          if(resultPhone.get("Records").get(0).has("Results")) {
            if(!resultPhone.get("Records").get(0).get("Results").textValue().equalsIgnoreCase("null")) {
              Results = resultPhone.get("Records").get(0).get("Results").textValue()
            }
          }
          if(resultPhone.get("Records").get(0).has("State")) {
            if(!resultPhone.get("Records").get(0).get("State").textValue().equalsIgnoreCase("null")) {
              State = resultPhone.get("Records").get(0).get("State").textValue()
            }
          }
        }
      }

      PhoneList.append(MelissaReverseLookupRecord(
        standardised_phone_number,
        AddressExtras,
        AddressKey,
        AddressLine1,
        AddressLine2,
        City,
        CompanyName,
        EmailAddress,
        MelissaAddressKey,
        NameFull,
        PhoneNumber,
        PostalCode,
        RecordExtras,
        RecordID,
        Reserved,
        Results,
        State))
    }
    }catch {
        case e: Exception => {

          println("ERROR:Exception in response got from aws call:"+response+"\n for phone_number"+standardised_phone_number+e.toString())

          e.printStackTrace();
          e.toString()
        }
      }
  }
}
