package com.ca.ci.enrichment

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.util.{Calendar, Properties}
import java.text.SimpleDateFormat
/**
  * Created by punma03 on 3/31/2017.
  */
object ZoomMelissaEnrichmentApiPhone {
  /*
    case class and as data type to store fields
     */
  case class Records(standardised_phone_number:String,
                     RecordID:String ,
                     Results:String,
                     PhoneNumber:String,
                     AdministrativeArea:String,
                     CountryAbbreviation:String,
                     CountryName:String,
                     Carrier:String,
                     CallerID:String,
                     DST:String,
                     InternationalPhoneNumber:String,
                     Language:String,
                     Latitude:String,
                     Locality:String,
                     Longitude:String,
                     PhoneInternationalPrefix:String,
                     PhoneCountryDialingCode:String,
                     PhoneNationPrefix:String,
                     PhoneNationalDestinationCode:String,
                     PhoneSubscriberNumber:String,
                     UTC:String,
                     PostalCode:String)

  var RecordsApi:scala.collection.mutable.ListBuffer[Records]=scala.collection.mutable.ListBuffer.empty

  val appname = "ZoomMelissaEnrichmentApiPhone"

  case class PhoneDetails(telephone_number:String,standardised_phone_number:String,is_valid_flag:String,country_code:String,person_phone_key:String)

  var phoneStandardizationList:scala.collection.mutable.ListBuffer[PhoneDetails]=scala.collection.mutable.ListBuffer.empty



  var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty
  var filePath:String=null
  var myConfigFile:String=null

  def main(arg: Array[String]) = {

    val sparkConf = new SparkConf().setAppName(appname)
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().appName(appname).getOrCreate()


    filePath=arg(0)
    myConfigFile=arg(1)
    /*
    val sparkConf = new SparkConf().setAppName(appname).setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder.appName("HygienePhoneEnrichment").config("spark.sql.warehouse.dir","file:///C:/tempwarehouse").getOrCreate()
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

    val entitydetail = sparkSession.read.jdbc(jdbcURL, "mdm.entity_detail", connectionProperties).select("entity_detail_key").where(" entity_name='Hygiene_Phone_Enrichment' ")
    // print("entitydetailkey"+entitydetailkey.collect().toList)

    val entity_detail_key=entitydetail.collect().toList(0).get(0).toString
    // print("entity_detail_key: "+entity_detail_key)

    val file_key=1
    val instanceName="Zoom_Melissa_EnrichmentApiPhone_load_"+System.currentTimeMillis()

    val jobStatusStart = sparkSession.sql("""select distinct """+maxJobKey+""" as batch_id,'"""+instanceName+"""' as instance_name,'started' as status,'"""+file_key+"""' as file_key ,
   '"""+entity_detail_key+"""' as entity_detail_key ,current_timestamp() as created_date,'"""+userName+"""' as created_user
    from jobs_master_table a """)
    //print(jobStatusStart.show())
    jobStatusStart.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)

    processRecords(sparkSession,ruleMap,jdbcURL,connectionProperties)
    /*
     saving spark job start and end time in  spark_jobs_run_details
    */
    val jobStatusEnd = sparkSession.sql("select distinct "+maxJobKey+" as batch_id,'"+instanceName+"'"+" as instance_name,'ended' as status,'"+file_key+"' as file_key , '"+entity_detail_key+"' as entity_detail_key ,current_timestamp() as created_date,'"+userName+"' as created_user from jobs_master_table")
    jobStatusEnd.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)
    val endTime = System.currentTimeMillis()

  }

  def processRecords(sparkSession: SparkSession,ruleMap:scala.collection.mutable.Map[String, String],jdbcURL:String,connectionProperties:Properties): Unit = {
     /*
    reading config files value
     */
    val userName=ruleMap.get("userName").get
    val melissaurl=ruleMap.get("melissaurl").get
    val melissahost=ruleMap.get("melissahost").get
    val melissacanonicaluri=ruleMap.get("melissacanonicaluri").get

    val access_key=ruleMap.get("access_key").get
    val secret_key=ruleMap.get("secret_key").get

    val throttlingFactormelissa =ruleMap.get("throttlingFactormelissa").get
    val timeToWaitInsecondmelissa = ruleMap.get("timeToWaitInsecondmelissa").get

    val backDateDaysMelissa=ruleMap.get("backDateDaysMelissa").get
    println("throttlingFactormelissa: "+throttlingFactormelissa)
    try {

       val dbEnrichmentApiPersonPhone = sparkSession.read.jdbc(jdbcURL, "mdm.enrichment_api_person_phone", connectionProperties).select("enrichment_api_person_key","phone_number","enrichment_api_person_phone_key","updated_date").where("phone_number is not null")

       val dbenrichmentapipersonemailpersonkey=dbEnrichmentApiPersonPhone.select(dbEnrichmentApiPersonPhone("enrichment_api_person_key").alias("person_key"),dbEnrichmentApiPersonPhone("phone_number").alias("telephone_number"),dbEnrichmentApiPersonPhone("enrichment_api_person_phone_key").alias("person_phone_key"),dbEnrichmentApiPersonPhone("updated_date"))


       /*
     get current date
     */
       val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
       val now = Calendar.getInstance().getTime()
       val currentDate = dateFormatter.format(now)
       /*
      get only current date records from PersonRunDetailDF or person table
       */
       dbenrichmentapipersonemailpersonkey.createOrReplaceTempView("Zoom_MelissaEnrichmentApi_Phone_Temp_View")
       val dbPhoneRunfilter = sparkSession.sql("select * from Zoom_MelissaEnrichmentApi_Phone_Temp_View where updated_date like'" + currentDate + "%'")
       println("Current Date Records from enrichment_api_person_email "+dbPhoneRunfilter.count())

       //val dbbridgepersonaddress = sparkSession.read.jdbc(jdbcURL, "mdm.bridge_person_address", connectionProperties).select("address_key","person_key")
       //val dbaddress = sparkSession.read.jdbc(jdbcURL, "mdm.address", connectionProperties).select("address_key","country")

       //val countrysynonym = sparkSession.read.jdbc(jdbcURL, "mdm.country_synonym", connectionProperties).select("country_synonym","country_code")
       //val countrysynonymCountry = countrysynonym.select(countrysynonym("country_synonym").alias("country"),countrysynonym("country_code"))

       //val dbEnrichPersonPhone=dbPhoneRunfilter.join(dbbridgepersonaddress,"person_key")
       //val dbBridgeAddress=dbEnrichPersonPhone.join(dbaddress,"address_key")
       //val dbAddressCountry=dbBridgeAddress.join(countrysynonymCountry,"country")

       print("dbAddressCountry**"+dbPhoneRunfilter.count())


         /*
        validate the phone number with google Library
        */
       dbPhoneRunfilter.toDF().collect().foreach(x => phoneValidation(x.getAs("telephone_number"),x.getAs("person_phone_key").toString,null))



       import sparkSession.implicits._
       val phoneDetails=phoneStandardizationList.toDF()
       print("phoneDetails**"+phoneDetails.show())

       val filteredBackdatedRecords=HygienePhoneEnrichment.phoneEnrichment(sparkSession,phoneDetails,backDateDaysMelissa,jdbcURL,connectionProperties)
       print("filteredBackdatedRecords**"+filteredBackdatedRecords.show())


       filteredBackdatedRecords.createOrReplaceTempView("StandardizationPhoneListMelissaEnrichmentApiPhone")

       val verification_reason_key=50000001
       val googleStandardizationHistory=sparkSession.sql("select person_phone_key,standardised_phone_number,is_valid_flag,'"+verification_reason_key+"' as verification_reason_key,telephone_number,current_timestamp() as created_date,'"+userName+"' as created_user_id,current_timestamp() as updated_date,'"+userName+"' as updated_user_id from StandardizationPhoneListMelissaEnrichmentApiPhone")
      //print("googlestandardizationhistory"+googleStandardizationHistory.show())

       /*
       store the list in person_phone_google_standardiration_history
       */
       googleStandardizationHistory.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.person_phone_google_standardization_history", connectionProperties)

       val fireDF=sparkSession.sql("select distinct standardised_phone_number,country_code from StandardizationPhoneListMelissaEnrichmentApiPhone")

       val filterPhoneList = fireDF.toDF().collect()
       print("Number of records to fire"+fireDF.count())
       invokeAPIWithThrottling(filterPhoneList,access_key,secret_key,melissaurl,melissahost,melissacanonicaluri,throttlingFactormelissa,timeToWaitInsecondmelissa)
    } catch {
      case e: Exception => {
        print("ERROR:Exception hand in processRecords"+e.toString())
        e.printStackTrace();
      }
    }finally
    {
      /*
      Saving the final result in enrichment_phone_temp and google api validated phone in phone validate google table
       */
      import sparkSession.implicits._
      import sparkSession.implicits._
      val RecordsApiDF = RecordsApi.toDF()
      if(!RecordsApi.isEmpty) {
        RecordsApiDF.createOrReplaceTempView("RecordsApiDFMelissaEnrichmentTempTable")
        val phoneToTempDF = sparkSession.sql("select distinct a.*,b.person_phone_key,current_timestamp() as phone_verification_date  from RecordsApiDFMelissaEnrichmentTempTable a,StandardizationPhoneListMelissaEnrichmentApiPhone b where a.standardised_phone_number = b.standardised_phone_number")
        val dropColumnDF = phoneToTempDF.drop("standardised_phone_number")
        dropColumnDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.enrichment_api_person_phone_verification_api_history_temp", connectionProperties)
      }
      }

  }

  def invokeAPIWithThrottling(dataArray:Array[org.apache.spark.sql.Row],access_key:String,secret_key:String,melissaurl:String,melissahost:String,melissacanonicaluri:String,throttlingFactormelissa:String,timeToWaitInsecondmelissa:String)=
  {
    import scala.collection.parallel._
    val dataList = dataArray.toList
    if(!dataList.isEmpty)
    {
      val parallelList =dataList.par
      parallelList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(throttlingFactormelissa.toInt))
      parallelList.map(x=>invokeAPI(x,access_key,secret_key,melissaurl,melissahost,melissacanonicaluri))
    }

  }

  def invokeAPI(row:org.apache.spark.sql.Row,access_key:String,secret_key:String,melissaurl:String,melissahost:String,melissacanonicaluri:String)
  {
    phoneVerification(row.getAs("standardised_phone_number"),com.ca.ci.enrichment.AWSRequestSigner.melissaPhoneVerification(access_key,secret_key,melissaurl,melissahost,melissacanonicaluri,row.getAs("standardised_phone_number"),row.getAs("country_code")) )//country_code
  }

  /*
  method to get the responce from AWSRequestSigner and process the records and store in the list
   */
  def phoneVerification(standardised_phone_number:String,response:String): Unit =
  {
    try {
      if (response!=null) {
        //print("inside melissa phoneVerification\n" + person_phone_key)
        /*
        parsing the responce in json format to read in a tree structure
        */
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        val resultPhone = mapper.readTree(response)

        var RecordID: String = null
        var Results: String = null
        var PhoneNumber: String = null
        var AdministrativeArea: String = null
        var CountryAbbreviation: String = null
        var CountryName: String = null
        var Carrier: String = null
        var CallerID: String = null
        var DST: String = null
        var InternationalPhoneNumber: String = null
        var Language: String = null
        var Latitude: String = null
        var Locality: String = null
        var Longitude: String = null
        var PhoneInternationalPrefix: String = null
        var PhoneCountryDialingCode: String = null
        var PhoneNationPrefix: String = null
        var PhoneNationalDestinationCode: String = null
        var PhoneSubscriberNumber: String = null
        var UTC: String = null
        var PostalCode: String = null

        /*
       get the fields from rest api and store in the list
       */
        if (resultPhone.has("Records")) {
          if (resultPhone.get("Records").has(0)) {

            if (resultPhone.get("Records").get(0).has("RecordID")) {
              if(!resultPhone.get("Records").get(0).get("RecordID").asText().equalsIgnoreCase("null")) {
                RecordID = resultPhone.get("Records").get(0).get("RecordID").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("Results")) {
              if(!resultPhone.get("Records").get(0).get("Results").asText().equalsIgnoreCase("null")) {
                Results = resultPhone.get("Records").get(0).get("Results").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("PhoneNumber")) {
              if(!resultPhone.get("Records").get(0).get("PhoneNumber").asText().equalsIgnoreCase("null")) {
                PhoneNumber = resultPhone.get("Records").get(0).get("PhoneNumber").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("AdministrativeArea")) {
              if(!resultPhone.get("Records").get(0).get("AdministrativeArea").asText().equalsIgnoreCase("null")) {
                AdministrativeArea = resultPhone.get("Records").get(0).get("AdministrativeArea").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("CountryAbbreviation")) {
              if(!resultPhone.get("Records").get(0).get("CountryAbbreviation").asText().equalsIgnoreCase("null")) {
                CountryAbbreviation = resultPhone.get("Records").get(0).get("CountryAbbreviation").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("CountryName")) {
              if(!resultPhone.get("Records").get(0).get("CountryName").asText().equalsIgnoreCase("null")) {
                CountryName = resultPhone.get("Records").get(0).get("CountryName").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("Carrier")) {
              if(!resultPhone.get("Records").get(0).get("Carrier").asText().equalsIgnoreCase("null")) {
                Carrier = resultPhone.get("Records").get(0).get("Carrier").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("CallerID")) {
              if(!resultPhone.get("Records").get(0).get("CallerID").asText().equalsIgnoreCase("null")) {
                CallerID = resultPhone.get("Records").get(0).get("CallerID").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("DST")) {
              if(!resultPhone.get("Records").get(0).get("DST").asText().equalsIgnoreCase("null")) {
                DST = resultPhone.get("Records").get(0).get("DST").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("InternationalPhoneNumber")) {
              if(!resultPhone.get("Records").get(0).get("InternationalPhoneNumber").asText().equalsIgnoreCase("null")) {
                InternationalPhoneNumber = resultPhone.get("Records").get(0).get("InternationalPhoneNumber").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("Language")) {
              if(!resultPhone.get("Records").get(0).get("Language").asText().equalsIgnoreCase("null")) {
                Language = resultPhone.get("Records").get(0).get("Language").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("Latitude")) {
              if(!resultPhone.get("Records").get(0).get("Latitude").asText().equalsIgnoreCase("null")) {
                Latitude = resultPhone.get("Records").get(0).get("Latitude").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("Locality")) {
              if(!resultPhone.get("Records").get(0).get("Locality").asText().equalsIgnoreCase("null")) {
                Locality = resultPhone.get("Records").get(0).get("Locality").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("Longitude")) {
              if(!resultPhone.get("Records").get(0).get("Longitude").asText().equalsIgnoreCase("null")) {
                Longitude = resultPhone.get("Records").get(0).get("Longitude").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("PhoneInternationalPrefix")) {
              if(!resultPhone.get("Records").get(0).get("PhoneInternationalPrefix").asText().equalsIgnoreCase("null")) {
                PhoneInternationalPrefix = resultPhone.get("Records").get(0).get("PhoneInternationalPrefix").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("PhoneCountryDialingCode")) {
              if(!resultPhone.get("Records").get(0).get("PhoneCountryDialingCode").asText().equalsIgnoreCase("null")) {
                PhoneCountryDialingCode = resultPhone.get("Records").get(0).get("PhoneCountryDialingCode").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("PhoneNationPrefix")) {
              if(!resultPhone.get("Records").get(0).get("PhoneNationPrefix").asText().equalsIgnoreCase("null")) {
                PhoneNationPrefix = resultPhone.get("Records").get(0).get("PhoneNationPrefix").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("PhoneNationalDestinationCode")) {
              if(!resultPhone.get("Records").get(0).get("PhoneNationalDestinationCode").asText().equalsIgnoreCase("null")) {
                PhoneNationalDestinationCode = resultPhone.get("Records").get(0).get("PhoneNationalDestinationCode").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("PhoneSubscriberNumber")) {
              if(!resultPhone.get("Records").get(0).get("PhoneSubscriberNumber").asText().equalsIgnoreCase("null")) {
                PhoneSubscriberNumber = resultPhone.get("Records").get(0).get("PhoneSubscriberNumber").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("UTC")) {
              if(!resultPhone.get("Records").get(0).get("UTC").asText().equalsIgnoreCase("null")) {
                UTC = resultPhone.get("Records").get(0).get("UTC").asText()
              }
            }
            if (resultPhone.get("Records").get(0).has("PostalCode")) {
              if(!resultPhone.get("Records").get(0).get("PostalCode").asText().equalsIgnoreCase("null")) {
                PostalCode = resultPhone.get("Records").get(0).get("PostalCode").asText()
              }
            }

            RecordsApi.append(Records(standardised_phone_number.toString,
              RecordID,
              Results,
              PhoneNumber,
              AdministrativeArea,
              CountryAbbreviation,
              CountryName,
              Carrier,
              CallerID,
              DST,
              InternationalPhoneNumber,
              Language,
              Latitude,
              Locality,
              Longitude,
              PhoneInternationalPrefix,
              PhoneCountryDialingCode,
              PhoneNationPrefix,
              PhoneNationalDestinationCode,
              PhoneSubscriberNumber,
              UTC,
              PostalCode))
            //println("\nRecordsApi RecordsApi:" + RecordsApi)
          }
        }
      }
    } catch {

      case e: Exception => {
        println("Error:Exception in method phoneVerification response got from aws call is:"+response+"\nphone_number"+standardised_phone_number+ e.toString())
        e.printStackTrace();
      }
    }
  }
   /*
    method will call Phonevalidationgoogle object method
   */
  def phoneValidation(telephone_number:String,person_phone_key:String,country_code:String): Unit =
  {
    val response = PhoneValidationGoogle.googlePhoneValidation(telephone_number,person_phone_key,country_code)
    if(response!=null) {
      phoneStandardizationList.append(PhoneDetails(response.telephone_number, response.standardised_phone_number.toString, response.isValid.toString, response.country_code, response.person_phone_key))
    }
  }
}
