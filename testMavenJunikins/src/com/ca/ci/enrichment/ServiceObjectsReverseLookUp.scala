package com.ca.ci.enrichment

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by punma03 on 3/12/2017.
  */
object ServiceObjectsReverseLookUp {
  /*
   map for property file
    */
  var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty
  val appname = "ServiceObjectsReverseLookUp"
  val file_key = 1
  val started = "started"
  val ended = "ended"
  val instanceName = appname + System.currentTimeMillis()
  case class googlePhoneDetails(telephone_number:String,standardised_phone_number:String,is_valid_flag:String,country:String,person_phone_key:String)
  var googleStandardizationList:scala.collection.mutable.ListBuffer[googlePhoneDetails]=scala.collection.mutable.ListBuffer.empty
  /*
  case class to store the phone records
  */
  case class phoneDetails(phonenumber:String,
                          ProviderName:String,
                          ProviderCity:String,
                          ProviderState:String,
                          ProviderLatitude:String,
                          ProviderLongitude:String,
                          ProviderLineType:String,
                          ContactsName:String,
                          ContactsAddress:String,
                          ContactsCity:String,
                          ContactsState:String,
                          ContactsPostalCode:String,
                          ContactsPhoneType:String,
                          ContactsLatitude:String,
                          ContactsLongitude:String,
                          ContactsSICCode:String,
                          ContactsSICDesc:String,
                          ContactsQualityScore:String,
                          SMSAddress:String,
                          MMSAddress:String,
                          DateFirstSeen:String,
                          DateOfPorting:String,
                          NoteCodes:String,
                          NoteDescriptions:String,
                          TokensUsed:String
                         )
  var phoneDetailList:scala.collection.mutable.ListBuffer[phoneDetails]=scala.collection.mutable.ListBuffer.empty
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
   /* val sparkConf = new SparkConf().setAppName(appname).setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder.appName(appname).config("spark.sql.warehouse.dir", "file:///C:/tempwarehouse").getOrCreate()*/
   /*
   read config file
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

    val entitydetail = sparkSession.read.jdbc(jdbcURL, "mdm.entity_detail", connectionProperties).select("entity_detail_key").where(" entity_name='service_object_reverselookup_phone_validation_temp' ")
    val entity_detail_key=entitydetail.collect().toList(0).get(0).toString

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


    //print("ruleMap" + ruleMap)
    sparkSession.stop()
    sparkContext.stop()
  }

  /*
mehod will accept the spark session and process the reords
 */
  def processRecords(sparkSession: SparkSession,ruleMap:scala.collection.mutable.Map[String, String],jdbcURL:String,connectionProperties:Properties): Unit = {
  /*
  getting config file values
   */
    //val jdbcURL=ruleMap.get("jdbcURL").get
    //val dbUser=ruleMap.get("dbUser").get
    val userName=ruleMap.get("userName").get
    //val dbPwd = readEnrichmentPropFile.getpassval(sparkSession,"rwpass",myConfigFile)
    //val connectionProperties = new Properties()
    //connectionProperties.put("user", dbUser)
    //connectionProperties.put("password", dbPwd)

    val ServiceObjectsReverseLookUpUrl=ruleMap.get("ServiceObjectsReverseLookUpUrl").get
    val ServiceObjectsReverseLookUphost=ruleMap.get("ServiceObjectsReverseLookUphost").get
    val ServiceObjectsReverseLookUpcanonicaluri=ruleMap.get("ServiceObjectsReverseLookUpcanonicaluri").get

    val throttlingFactorServiceObjectsReverseLookUp =ruleMap.get("throttlingFactorServiceObjectsReverseLookUp").get
    val timeToWaitInsecondServiceObjectsReverseLookUp = ruleMap.get("timeToWaitInsecondServiceObjectsReverseLookUp").get

    println("throttlingFactorServiceObjectsReverseLookUp" + throttlingFactorServiceObjectsReverseLookUp)

    val access_key=ruleMap.get("access_key").get
    val secret_key=ruleMap.get("secret_key").get
    val backDateDays=ruleMap.get("backDateDaysServiceObjectsReverseLookUp").get
    try {
    /*
      Get the 15 days back date from current date
     */
    val cal: Calendar = Calendar.getInstance();
    cal.add(Calendar.DATE, backDateDays.toInt)
    val laterdate = cal.getTime
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    val backDate = dateFormatter.format(laterdate)
    /*
   loading batch_run_details_view and person_phone_verification_serviceobject_api_history in memory
 */
    val dbPersonPhone = sparkSession.read.jdbc(jdbcURL, "mdm.enrichment_api_melissadata_reverse_phone_temp", connectionProperties).select("person_phone_key", "PhoneNumber","Results")//.where("person_key in (606732,558912,535914,550900,463955,458338,457453,452415,441254,415033) ")
    val filterrecordsforServiceobject=dbPersonPhone.filter(dbPersonPhone("Results").contains("PS02") or dbPersonPhone("Results").contains("PS16"))

    val dbserviceobjecthistory = sparkSession.read.jdbc(jdbcURL, "mdm.person_phone_verification_serviceobject_api_history", connectionProperties).select("person_phone_key","phone_number","updated_date")
    //val dbenrichmentapihistory = sparkSession.read.jdbc(jdbcURL, "mdm.enrichment_api_serviceobject_reverse_phone", connectionProperties).select("person_phone_key","updated_date")
    
    //print("dbPersonPhone" + dbPersonPhone.show())
    /*
    remove the records which are processed 15 days back
    */
    dbserviceobjecthistory.createOrReplaceTempView("dbserviceobjectreverselookuphistory")
    val dbServiceObjectHistoryFilter = sparkSession.sql("select person_phone_key from dbserviceobjectreverselookuphistory where updated_date  >='" + backDate + "'")
    //print("records to be filter from history table" + dbServiceObjectHistoryFilter.show())
    /*
    remove the records from batch_run_details_view
     */
    val filterRecord = filterrecordsforServiceobject.select("person_phone_key")
    val finalResult = filterRecord.except(dbServiceObjectHistoryFilter)
    //print("\nafter filter records from history table" + finalResult.show())

    /*
    join back to get all columns
     */
    val finalDF=filterrecordsforServiceobject.join(finalResult,"person_phone_key")
    //print("\nbefore google validation" + finalDF.show())
    //print("\nbefore google validation" + finalDF.count())
    val finalresultList = finalDF.toDF().collect()

    /*
    validate the phone number with google Library
     */
    finalresultList.foreach(x => phoneValidation(x.getAs("PhoneNumber"),x.getAs("person_phone_key").toString,"US"))

    import sparkSession.implicits._
    val googleStandardizationDF=googleStandardizationList.toDF()
    //print("\nafter google before distinct"+googleStandardizationDF.show())
    //print("\nafter google before distinct"+googleStandardizationDF.count())

    googleStandardizationDF.createOrReplaceTempView("googleStandardizationDFServcieObjectReverse")
    val filteredDuplicateDF=sparkSession.sql("select distinct standardised_phone_number from googleStandardizationDFServcieObjectReverse")


    //print("\nafter distinct"+filteredDuplicateDF.show())
    //print("\nafter count"+filteredDuplicateDF.count())

    invokeAPIWithThrottling(filteredDuplicateDF.take(10),access_key,secret_key,ServiceObjectsReverseLookUpUrl,ServiceObjectsReverseLookUphost,ServiceObjectsReverseLookUpcanonicaluri,throttlingFactorServiceObjectsReverseLookUp,timeToWaitInsecondServiceObjectsReverseLookUp)

    }catch
  {
    case e: Exception => {
      print("Exception for query:"+e.toString())
      e.printStackTrace();
    }
  }
  finally {


    /*
  finally store the data in temp table
   */
    import sparkSession.implicits._
    val phoneDetailListDF = phoneDetailList.toDF()
    phoneDetailListDF.createOrReplaceTempView("ServiceObjectReversePhoneDetailListDF")
    //println("RecordsApiDF" + phoneDetailListDF.show())

    val phoneToTempDF = sparkSession.sql("select distinct a.*,b.person_phone_key,current_timestamp() as phone_verification_date from ServiceObjectReversePhoneDetailListDF a,googleStandardizationDFServcieObjectReverse b where a.phonenumber = b.standardised_phone_number")
    //println("phoneToTempDF" + phoneToTempDF.show())

    phoneToTempDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.service_object_reverselookup_phone_validation_temp", connectionProperties)

    /*val googleFireDF=sparkSession.sql("select * from googleStandardizationDFServcieObjectReverse")
    print("melissaFireDF count"+googleFireDF.show())*/
    val verification_reason_key=50000001
    /*
    naming change for variable
     */
    val googleStandardizationHistory=sparkSession.sql("select person_phone_key,standardised_phone_number,is_valid_flag,'"+verification_reason_key+"' as verification_reason_key,telephone_number,current_timestamp() as created_date,'"+userName+"' as created_user_id,current_timestamp() as updated_date,'"+userName+"' as updated_user_id from googleStandardizationDFServcieObjectReverse")
    //print("googlestandardizationhistory"+googleStandardizationHistory.show())
    /*
    store the list in person_phone_google_standardiration_history
     */
   googleStandardizationHistory.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.person_phone_google_standardization_history", connectionProperties)
}
}


/*
throttling algo mehod
*/
def invokeAPIWithThrottling(dataArray:Array[org.apache.spark.sql.Row],access_key:String,secret_key:String,ServiceObjectsReverseLookUpUrl:String,ServiceObjectsReverseLookUphost:String,ServiceObjectsReverseLookUpcanonicaluri:String,throttlingFactorServiceObjectsReverseLookUp:String,timeToWaitInsecondServiceObjectsReverseLookUp:String)=
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
    parallelList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(throttlingFactorServiceObjectsReverseLookUp.toInt))
    parallelList.map(x=>invokeAPI(x,access_key,secret_key,ServiceObjectsReverseLookUpUrl,ServiceObjectsReverseLookUphost,ServiceObjectsReverseLookUpcanonicaluri))
    // parallelList.
  }
  //val endTime = System.currentTimeMillis()
  //println("Overall Time taken to process all records :"+(endTime-startTime )/1000)

}

/*
invokeAPI method will call the aws api
*/
def invokeAPI(row:org.apache.spark.sql.Row,access_key:String,secret_key:String,ServiceObjectsReverseLookUpUrl:String,ServiceObjectsReverseLookUphost:String,ServiceObjectsReverseLookUpcanonicaluri:String)
{
createRecordFromJSONResponseObject(row.getAs("standardised_phone_number"),com.ca.ci.enrichment.AWSRequestSigner.ServiceObjectsReverseLookUpApi(access_key,secret_key,ServiceObjectsReverseLookUpUrl,ServiceObjectsReverseLookUphost,ServiceObjectsReverseLookUpcanonicaluri,row.getAs("standardised_phone_number")))//"847-264-5802"))//row.getAs("telephone_number")))
}

/**
* This method would parse the JSON response and create a record
*
* @return
*/
def createRecordFromJSONResponseObject(phonenumber:String,response:String) = {
  try {
    if (response != null) {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val resultPhone = mapper.readTree(response)

      var ProviderName: String = null
      var ProviderCity: String = null
      var ProviderState: String = null
      var ProviderLatitude: String = null
      var ProviderLongitude: String = null
      var ProviderLineType: String = null

      var ContactsName: String = null
      var ContactsAddress: String = null
      var ContactsCity: String = null
      var ContactsState: String = null
      var ContactsPostalCode: String = null
      var ContactsPhoneType: String = null
      var ContactsLatitude: String = null
      var ContactsLongitude: String = null
      var ContactsSICCode: String = null
      var ContactsSICDesc: String = null
      var ContactsQualityScore: String = null

      var SMSAddress: String = null
      var MMSAddress: String = null
      var DateFirstSeen: String = null
      var DateOfPorting: String = null
      var NoteCodes: String = null
      var NoteDescriptions: String = null
      var TokensUsed: String = null

      if (resultPhone.has("PhoneInfo")) {
        if (resultPhone.get("PhoneInfo").has("Provider")) {

          if (resultPhone.get("PhoneInfo").get("Provider").has("Name")) {
            if (!resultPhone.get("PhoneInfo").get("Provider").get("Name").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Provider").get("Name").asText().equalsIgnoreCase("")) {
              ProviderName = resultPhone.get("PhoneInfo").get("Provider").get("Name").asText()
            }
          }
          if (resultPhone.get("PhoneInfo").get("Provider").has("City")) {
            if (!resultPhone.get("PhoneInfo").get("Provider").get("City").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Provider").get("City").asText().equalsIgnoreCase("")) {
              ProviderCity = resultPhone.get("PhoneInfo").get("Provider").get("City").asText()
            }
          }
          if (resultPhone.get("PhoneInfo").get("Provider").has("State")) {
            if (!resultPhone.get("PhoneInfo").get("Provider").get("State").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Provider").get("State").asText().equalsIgnoreCase("")) {
              ProviderState = resultPhone.get("PhoneInfo").get("Provider").get("State").asText()
            }
          }
          if (resultPhone.get("PhoneInfo").get("Provider").has("Latitude")) {
            if (!resultPhone.get("PhoneInfo").get("Provider").get("Latitude").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Provider").get("Latitude").asText().equalsIgnoreCase("")) {
              ProviderLatitude = resultPhone.get("PhoneInfo").get("Provider").get("Latitude").asText()
            }
          }
          if (resultPhone.get("PhoneInfo").get("Provider").has("Longitude")) {
            if (!resultPhone.get("PhoneInfo").get("Provider").get("Longitude").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Provider").get("Longitude").asText().equalsIgnoreCase("")) {
              ProviderLongitude = resultPhone.get("PhoneInfo").get("Provider").get("Longitude").asText()
            }
          }
          if (resultPhone.get("PhoneInfo").get("Provider").has("LineType")) {
            if (!resultPhone.get("PhoneInfo").get("Provider").get("LineType").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Provider").get("LineType").asText().equalsIgnoreCase("")) {
              ProviderLineType = resultPhone.get("PhoneInfo").get("Provider").get("LineType").asText()
            }
          }
        }

        if (resultPhone.get("PhoneInfo").has("Contacts")) {

          if (resultPhone.get("PhoneInfo").get("Contacts").has(0)) {

            if (resultPhone.get("PhoneInfo").get("Contacts").get(0).has("Name")) {
              if (!resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Name").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Name").asText().equalsIgnoreCase("")) {
                ContactsName = resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Name").asText()
              }
            }
            if (resultPhone.get("PhoneInfo").get("Contacts").get(0).has("Address")) {
              if (!resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Address").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Address").asText().equalsIgnoreCase("")) {
                ContactsCity = resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Address").asText()
              }
            }
            if (resultPhone.get("PhoneInfo").get("Contacts").get(0).has("City")) {
              if (!resultPhone.get("PhoneInfo").get("Contacts").get(0).get("City").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Contacts").get(0).get("City").asText().equalsIgnoreCase("")) {
                ContactsState = resultPhone.get("PhoneInfo").get("Contacts").get(0).get("City").asText()
              }
            }
            if (resultPhone.get("PhoneInfo").get("Contacts").get(0).has("State")) {
              if (!resultPhone.get("PhoneInfo").get("Contacts").get(0).get("State").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Contacts").get(0).get("State").asText().equalsIgnoreCase("")) {
                ContactsLatitude = resultPhone.get("PhoneInfo").get("Contacts").get(0).get("State").asText()
              }
            }
            if (resultPhone.get("PhoneInfo").get("Contacts").get(0).has("PostalCode")) {
              if (!resultPhone.get("PhoneInfo").get("Contacts").get(0).get("PostalCode").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Contacts").get(0).get("PostalCode").asText().equalsIgnoreCase("")) {
                ContactsLongitude = resultPhone.get("PhoneInfo").get("Contacts").get(0).get("PostalCode").asText()
              }
            }
            if (resultPhone.get("PhoneInfo").get("Contacts").get(0).has("Latitude")) {
              if (!resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Latitude").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Latitude").asText().equalsIgnoreCase("")) {
                ContactsLatitude = resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Latitude").asText()
              }
            }
            if (resultPhone.get("PhoneInfo").get("Contacts").get(0).has("Longitude")) {
              if (!resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Longitude").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Longitude").asText().equalsIgnoreCase("")) {
                ContactsLongitude = resultPhone.get("PhoneInfo").get("Contacts").get(0).get("Longitude").asText()
              }
            }
            if (resultPhone.get("PhoneInfo").get("Contacts").get(0).has("SICCode")) {
              if (!resultPhone.get("PhoneInfo").get("Contacts").get(0).get("SICCode").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Contacts").get(0).get("SICCode").asText().equalsIgnoreCase("")) {
                ContactsSICCode = resultPhone.get("PhoneInfo").get("Contacts").get(0).get("SICCode").asText()
              }
            }
            if (resultPhone.get("PhoneInfo").get("Contacts").get(0).has("SICDesc")) {
              if (!resultPhone.get("PhoneInfo").get("Contacts").get(0).get("SICDesc").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Contacts").get(0).get("SICDesc").asText().equalsIgnoreCase("")) {
                ContactsSICDesc = resultPhone.get("PhoneInfo").get("Contacts").get(0).get("SICDesc").asText()
              }
            }
            if (resultPhone.get("PhoneInfo").get("Contacts").get(0).has("QualityScore")) {
              if (!resultPhone.get("PhoneInfo").get("Contacts").get(0).get("QualityScore").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("Contacts").get(0).get("QualityScore").asText().equalsIgnoreCase("")) {
                ContactsQualityScore = resultPhone.get("PhoneInfo").get("Contacts").get(0).get("QualityScore").asText()
              }
            }
          }
        }

        if (resultPhone.get("PhoneInfo").has("SMSAddress")) {
          if (!resultPhone.get("PhoneInfo").get("SMSAddress").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("SMSAddress").asText().equalsIgnoreCase("")) {
            SMSAddress = resultPhone.get("PhoneInfo").get("SMSAddress").asText()
          }
        }
        if (resultPhone.get("PhoneInfo").has("MMSAddress")) {
          if (!resultPhone.get("PhoneInfo").get("MMSAddress").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("MMSAddress").asText().equalsIgnoreCase("")) {
            MMSAddress = resultPhone.get("PhoneInfo").get("MMSAddress").asText()
          }
        }
        if (resultPhone.get("PhoneInfo").has("DateFirstSeen")) {
          if (!resultPhone.get("PhoneInfo").get("DateFirstSeen").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("DateFirstSeen").asText().equalsIgnoreCase("")) {
            DateFirstSeen = resultPhone.get("PhoneInfo").get("DateFirstSeen").asText()
          }
        }
        if (resultPhone.get("PhoneInfo").has("DateOfPorting")) {
          if (!resultPhone.get("PhoneInfo").get("DateOfPorting").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("DateOfPorting").asText().equalsIgnoreCase("")) {
            DateOfPorting = resultPhone.get("PhoneInfo").get("DateOfPorting").asText()
          }
        }
        if (resultPhone.get("PhoneInfo").has("NoteCodes")) {
          if (!resultPhone.get("PhoneInfo").get("NoteCodes").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("NoteCodes").asText().equalsIgnoreCase("")) {
            NoteCodes = resultPhone.get("PhoneInfo").get("NoteCodes").asText()
          }
        }
        if (resultPhone.get("PhoneInfo").has("NoteDescriptions")) {
          if (!resultPhone.get("PhoneInfo").get("NoteDescriptions").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("NoteDescriptions").asText().equalsIgnoreCase("")) {
            NoteDescriptions = resultPhone.get("PhoneInfo").get("NoteDescriptions").asText()
          }
        }
        if (resultPhone.get("PhoneInfo").has("TokensUsed")) {
          if (!resultPhone.get("PhoneInfo").get("TokensUsed").asText().equalsIgnoreCase("null") && !resultPhone.get("PhoneInfo").get("TokensUsed").asText().equalsIgnoreCase("")) {
            TokensUsed = resultPhone.get("PhoneInfo").get("TokensUsed").asText()
          }
        }


        phoneDetailList.append(phoneDetails(
          phonenumber.toString,
          ProviderName,
          ProviderCity,
          ProviderState,
          ProviderLatitude,
          ProviderLongitude,
          ProviderLineType,
          ContactsName,
          ContactsAddress,
          ContactsCity,
          ContactsState,
          ContactsPostalCode,
          ContactsPhoneType,
          ContactsLatitude,
          ContactsLongitude,
          ContactsSICCode,
          ContactsSICDesc,
          ContactsQualityScore,
          SMSAddress,
          MMSAddress,
          DateFirstSeen,
          DateOfPorting,
          NoteCodes,
          NoteDescriptions,
          TokensUsed))
          //print("phoneDetailList" + phoneDetailList)

      }
    }
  }catch {

    case e: Exception => {
      print("\nERROR:Exception in response got from aws call:"+response+"\n phonenumber:"+phonenumber)
      e.printStackTrace();
      e.toString()
    }
  }
}



def phoneValidation(telephone_number:String,person_phone_key:String,country:String): Unit =
{
  val response = PhoneValidationGoogle.googlePhoneValidation(telephone_number,person_phone_key,country)
  if(response!=null) {
    googleStandardizationList.append(googlePhoneDetails(response.telephone_number, response.standardised_phone_number.toString, response.isValid.toString, response.country_code, response.person_phone_key))
  }
 }
}
