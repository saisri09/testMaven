package com.ca.ci.enrichment

import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
/**
  * Created by punma03 on 3/6/2017.
  */
object MelissaDataVerification {
  val appname = "MelissaDataEnrichment"
  val file_key = 1
  val userName = "punma03"
  val started = "started"
  val ended = "ended"
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
  val instanceName = appname + System.currentTimeMillis()
  /*
  main processing method will filter the records from ta
  */
  def processMelissaDataRecords(sparkSession:SparkSession,phoneDetails:DataFrame,access_key:String,secret_key:String,melissaurl:String,melissahost:String,melissacanonicaluri:String,throttlingFactormelissa:String,timeToWaitInsecondmelissa:String,jdbcURL:String,connectionProperties:Properties): Unit = {
    println("...inside MelissaDataEnrichment zoominfo...")
    try {

     /*
     check the duplicate in person_phone_google_standardization_history
     */
     val fireDF=sparkSession.sql("select distinct standardised_phone_number,country_code from StandardizationPhoneListMelissaHyginene")

      val filterPhoneList = fireDF.toDF().collect()
      //println("Number of records to fire in melissa: "+fireDF.show())
      println("Number of records to fire in melissa:count "+fireDF.count())

     invokeAPIWithThrottling(filterPhoneList.take(5),access_key,secret_key,melissaurl,melissahost,melissacanonicaluri,throttlingFactormelissa,timeToWaitInsecondmelissa)
    } catch {
      case e: Exception => {
        print("Exception:"+e.toString()+ e.printStackTrace())
      }
    }finally
    {
      /*
      Saving the final result in enrichment_phone_temp and google api validated phone in phone validate google table
       */
      import sparkSession.implicits._
      import sparkSession.implicits._
      if(!RecordsApi.isEmpty) {
        val RecordsApiDF = RecordsApi.toDF()
        RecordsApiDF.createOrReplaceTempView("RecordsApiDFMelissa")
        val phoneToTempDF = sparkSession.sql("select distinct a.*,b.person_phone_key,current_timestamp() as phone_verification_date  from RecordsApiDFMelissa a,StandardizationPhoneListMelissaHyginene b where a.standardised_phone_number = b.standardised_phone_number")
        val dropColumnDF = phoneToTempDF.drop("standardised_phone_number")
        dropColumnDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.person_phone_verification_api_history_temp", connectionProperties)
      }
    }
  }

  /*
  Throttling main method
   */
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
  /*
  calling AWSSigner method
  */
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
        }
      }
    }
    } catch {
      case e: Exception => {
        println("Error:Exception in method phoneVerification response got from aws call is:"+response+"\nphone_number"+standardised_phone_number+e.toString()+  e.printStackTrace())

      }
    }
  }
}
