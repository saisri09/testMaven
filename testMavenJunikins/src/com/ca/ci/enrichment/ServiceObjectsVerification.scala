package com.ca.ci.enrichment

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by punma03 on 3/8/2017.
  */
object ServiceObjectsVerification {
  val appname = "MelissaDataEnrichment"
  val file_key = 1
  val userName = "punma03"
  val started = "started"
  val ended = "ended"
  val instanceName = appname + System.currentTimeMillis()
  case class Records( Phone_Number:String,
                      phonenumber:String,
                      name:String,
                      city:String,
                      state:String,
                      country:String,
                      linetype:String,
                      timezone:String,
                      latitude:String,
                      longitude:String,
                      smsaddress:String,
                      mmsaddress:String,
                      originalname:String,
                      originallinetype:String,
                      porteddate:String,
                      lata:String,
                      notecodes:String,
                      notedescription:String)

  var serviceObjectList:scala.collection.mutable.ListBuffer[Records]=scala.collection.mutable.ListBuffer.empty

  def processServiceObjectsRecords(filterrecordsforServiceobject:DataFrame,sparkSession: SparkSession,access_key:String,secret_key:String,serviceObjectUrl:String,serviceObjecthost:String,serviceObjectcanonicaluri:String,throttlingFactorserviceObject:String,timeToWaitInsecondserviceObject:String,jdbcURL:String,connectionProperties:Properties,backDateDaysServiceObject:String): Unit = {

    println("...inside ServiceObjectsVerification zoominfo...")
    try {
      /*
      call throttling method by passing data frame
       */

      /*
    Get the 15 days back date from current date
    */
      val cal:Calendar  = Calendar.getInstance();
      cal.add(Calendar.DATE, backDateDaysServiceObject.toInt)
      val laterdate=cal.getTime
      //print("currentDate"+cal.getTime)
      val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
      val backDate = dateFormatter.format(laterdate)
      print("\nbackDateServiceObject"+backDate)

      val dbserviceobjecthistory = sparkSession.read.jdbc(jdbcURL, "mdm.person_phone_verification_serviceobject_api_history", connectionProperties).select("person_phone_key","phone_number","updated_date")
      dbserviceobjecthistory.createOrReplaceTempView("dbserviceobjectuphistory")
      val dbServiceObjectHistoryFilter = sparkSession.sql("select person_phone_key from dbserviceobjectuphistory where updated_date  >='" + backDate + "'")

      val filterServiceObjectDF=filterrecordsforServiceobject.select("person_phone_key")

      val filterBackDate=filterServiceObjectDF.except(dbServiceObjectHistoryFilter)

      val finalSODF=filterBackDate.join(filterrecordsforServiceobject,"person_phone_key")

      finalSODF.createOrReplaceTempView("filterrecordsforServiceobjectemptable")
      //distinct
      val distinctPhoneSoDF=sparkSession.sql("select distinct PhoneNumber from filterrecordsforServiceobjectemptable")
      //println("Number of records to fire in ServiceObjects: "+distinctPhoneSoDF.show())
      println("Number of records to fire in ServiceObjects: "+distinctPhoneSoDF.count())
      val recordsForServiceobject = distinctPhoneSoDF.toDF().collect()

      invokeAPIWithThrottling(recordsForServiceobject.take(5),access_key,secret_key,serviceObjectUrl,serviceObjecthost,serviceObjectcanonicaluri,throttlingFactorserviceObject,timeToWaitInsecondserviceObject)

    }finally {

      import sparkSession.implicits._
      val serviceObjectDF = serviceObjectList.toDF()
      if(!serviceObjectList.isEmpty) {

        serviceObjectDF.createOrReplaceTempView("serviceObjectPhoneDFTempTable")
        val phoneServiceObjectToTempDF = sparkSession.sql("select distinct a.*,b.person_phone_key,current_timestamp() as phone_verification_date from serviceObjectPhoneDFTempTable a,filterrecordsforServiceobjectemptable b where a.Phone_Number = b.PhoneNumber")
        val finalDropDFServiceObject = phoneServiceObjectToTempDF.drop("Phone_Number")
        finalDropDFServiceObject.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.service_object_phone_validation_temp", connectionProperties)

      }
    }
  }

  /*
throttling algo mehod
 */
  def invokeAPIWithThrottling(dataArray:Array[org.apache.spark.sql.Row],access_key:String,secret_key:String,serviceObjectUrl:String,serviceObjecthost:String,serviceObjectcanonicaluri:String,throttlingFactorserviceObject:String,timeToWaitInsecondserviceObject:String)=
  {
    import scala.collection.parallel._
    val dataList = dataArray.toList
    if(!dataList.isEmpty)
    {
      val parallelList =dataList.par
      parallelList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(throttlingFactorserviceObject.toInt))
      parallelList.map(x=>invokeAPI(x,access_key,secret_key,serviceObjectUrl,serviceObjecthost,serviceObjectcanonicaluri))
    }
  }

  /*
  invokeAPI method will call the aws api
   */
  def invokeAPI(row:org.apache.spark.sql.Row,access_key:String,secret_key:String,clearBitDomainUrl:String,clearBitDomainhost:String,clearBitDomaincanonicaluri:String)
  {
   serviceObjectPhoneValidation(row.getAs("PhoneNumber"),com.ca.ci.enrichment.AWSRequestSigner.serviceObjectPhoneVerification(access_key,secret_key,clearBitDomainUrl,clearBitDomainhost,clearBitDomaincanonicaluri,row.getAs("PhoneNumber")))
  }

  /*
  json response parser method
   */

  def serviceObjectPhoneValidation(Phone_Number:String,response:String): Unit = {
    try{
    if (response != null) {

       /*
       parsing the records in json
       */
      var PhoneNumber: String = null
      var Name: String = null
      var City: String = null
      var State: String = null
      var Country: String = null
      var LineType: String = null
      var TimeZone: String = null
      var Latitude: String = null
      var Longitude: String = null
      var SMSAddress: String = null
      var MMSAddress: String = null
      var OriginalName: String = null
      var OriginalLineType: String = null
      var PortedDate: String = null
      var LATA: String = null
      var NoteCodes: String = null
      var NoteDescriptions: String = null

      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val resultPhone = mapper.readTree(response)

      if (resultPhone.has("ExchangeInfoResults")) {
        if (resultPhone.get("ExchangeInfoResults").has(0)) {
          if(resultPhone.get("ExchangeInfoResults").get(0).has("PhoneNumber")) {
            if(!resultPhone.get("ExchangeInfoResults").get(0).get("PhoneNumber").asText().equalsIgnoreCase("null")) {
              PhoneNumber = resultPhone.get("ExchangeInfoResults").get(0).get("PhoneNumber").asText()
            }
          }
          if(resultPhone.get("ExchangeInfoResults").get(0).has("Name")) {
            if(!resultPhone.get("ExchangeInfoResults").get(0).get("Name").asText().equalsIgnoreCase("null")) {
              Name = resultPhone.get("ExchangeInfoResults").get(0).get("Name").asText()
            }
          }
          if(resultPhone.get("ExchangeInfoResults").get(0).has("City")) {
            if(!resultPhone.get("ExchangeInfoResults").get(0).get("City").asText().equalsIgnoreCase("null")) {
              City = resultPhone.get("ExchangeInfoResults").get(0).get("City").asText()
            }
          }
          if(resultPhone.get("ExchangeInfoResults").get(0).has("State")) {
            if(!resultPhone.get("ExchangeInfoResults").get(0).get("State").asText().equalsIgnoreCase("null")) {
              State = resultPhone.get("ExchangeInfoResults").get(0).get("State").asText()
            }
          }
          if(resultPhone.get("ExchangeInfoResults").get(0).has("Country")) {
            if(!resultPhone.get("ExchangeInfoResults").get(0).get("Country").asText().equalsIgnoreCase("null")) {
              Country = resultPhone.get("ExchangeInfoResults").get(0).get("Country").asText()
            }
          }
          if(resultPhone.get("ExchangeInfoResults").get(0).has("LineType")) {
            if(!resultPhone.get("ExchangeInfoResults").get(0).get("LineType").asText().equalsIgnoreCase("null")) {
              LineType = resultPhone.get("ExchangeInfoResults").get(0).get("LineType").asText()
            }
          }
          if(resultPhone.get("ExchangeInfoResults").get(0).has("TimeZone")) {
            if(!resultPhone.get("ExchangeInfoResults").get(0).get("TimeZone").asText().equalsIgnoreCase("null")) {
              TimeZone = resultPhone.get("ExchangeInfoResults").get(0).get("TimeZone").asText()
            }
          }
          if(resultPhone.get("ExchangeInfoResults").get(0).has("Latitude")) {
            if(!resultPhone.get("ExchangeInfoResults").get(0).get("Latitude").asText().equalsIgnoreCase("null")) {
              Latitude = resultPhone.get("ExchangeInfoResults").get(0).get("Latitude").asText()
            }
          }
          if(resultPhone.get("ExchangeInfoResults").get(0).has("Longitude")) {
            if(!resultPhone.get("ExchangeInfoResults").get(0).get("Longitude").asText().equalsIgnoreCase("null")) {
              Longitude = resultPhone.get("ExchangeInfoResults").get(0).get("Longitude").asText()
            }
          }
          if(resultPhone.get("ExchangeInfoResults").get(0).has("SMSAddress")) {
            if(!resultPhone.get("ExchangeInfoResults").get(0).get("SMSAddress").asText().equalsIgnoreCase("null")) {
              SMSAddress = resultPhone.get("ExchangeInfoResults").get(0).get("SMSAddress").asText()
            }
          }
          if(resultPhone.get("ExchangeInfoResults").get(0).has("MMSAddress")) {
            if(!resultPhone.get("ExchangeInfoResults").get(0).get("MMSAddress").asText().equalsIgnoreCase("null")) {
              MMSAddress = resultPhone.get("ExchangeInfoResults").get(0).get("MMSAddress").asText()
            }
          }
          if(resultPhone.get("ExchangeInfoResults").get(0).has("PortedInfo")) {

            if(resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").has("OriginalName")) {
              if(!resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").get("OriginalName").asText().equalsIgnoreCase("null")) {
                OriginalName = resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").get("OriginalName").asText()
              }
            }
            if(resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").has("OriginalLineType")) {
              if(!resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").get("OriginalLineType").asText().equalsIgnoreCase("null")) {
                OriginalLineType = resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").get("OriginalLineType").asText()
              }
            }
            if(resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").has("PortedDate")) {
              if(!resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").get("PortedDate").asText().equalsIgnoreCase("null")) {
                PortedDate = resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").get("PortedDate").asText()
              }
            }
            if(resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").has("LATA")) {
              if(!resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").get("LATA").asText().equalsIgnoreCase("null")) {
                LATA = resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").get("LATA").asText()
              }
            }
            if(resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").has("NoteCodes")) {
              if(!resultPhone.get("ExchangeInfoResults").get(0).get("NoteCodes").asText().equalsIgnoreCase("null")) {
                NoteCodes = resultPhone.get("ExchangeInfoResults").get(0).get("NoteCodes").asText()
              }
            }
            if(resultPhone.get("ExchangeInfoResults").get(0).get("PortedInfo").has("NoteDescriptions")) {
              if(!resultPhone.get("ExchangeInfoResults").get(0).get("NoteDescriptions").asText().equalsIgnoreCase("null")) {
                NoteDescriptions = resultPhone.get("ExchangeInfoResults").get(0).get("NoteDescriptions").asText()
              }
            }
          }

          serviceObjectList.append(Records(
            Phone_Number,
            PhoneNumber,
            Name,
            City,
            State,
            Country,
            LineType,
            TimeZone,
            Latitude,
            Longitude,
            SMSAddress,
            MMSAddress,
            OriginalName,
            OriginalLineType,
            PortedDate,
            LATA,
            NoteCodes,
            NoteDescriptions))
        }
      }
    } }catch {

      case e: Exception => {
        println("Error:Exception in method serviceObjectPhoneValidation response got from aws call is :"+response+"\nPhone_Number:"+Phone_Number+ e.toString()+e.printStackTrace())

      }
    }
  }
}
