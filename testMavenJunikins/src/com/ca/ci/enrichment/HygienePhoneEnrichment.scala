package com.ca.ci.enrichment

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  * Created by punma03 on 3/9/2017.
  */
object HygienePhoneEnrichment {
  /*
  phone enrichment rules to filter data fame from history tables and back date
   */
    def phoneEnrichment(sparkSession: SparkSession,dbPersonPhone:DataFrame,backDateDaysMelissa:String,jdbcURL:String,connectionProperties:Properties):DataFrame = {
     //print("in side phoneEnrichment :"+backDateDaysMelissa)
    /*
    Get the 15 days back date from current date
    */
    val cal:Calendar  = Calendar.getInstance();
    cal.add(Calendar.DATE, backDateDaysMelissa.toInt)
    val laterdate=cal.getTime
    //print("currentDate"+cal.getTime)
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    val backDate = dateFormatter.format(laterdate)
    print("\nbackDate"+backDate)
      /*
      Getting the Records from person_phone_verification_api_history,enrichment_api_person_phone_verification_api_history ,person_phone_verification_serviceobject_api_historyand filter it from person_phone_verification_api_history table
      */
      val personphoneverificationapihistory = sparkSession.read.jdbc(jdbcURL, "mdm.person_phone_verification_api_history", connectionProperties).select("phone_number","updated_date")
      val enrichmentapipersonphoneverificationapihistory = sparkSession.read.jdbc(jdbcURL, "mdm.enrichment_api_person_phone_verification_api_history", connectionProperties).select("phone_number","updated_date")
      val personphoneverificationerviceobjectapihistory = sparkSession.read.jdbc(jdbcURL, "mdm.person_phone_verification_serviceobject_api_history", connectionProperties).select("phone_number","updated_date")
      /*
      union all above three data frame
      */
      val allPhoneHistory=personphoneverificationapihistory.union(enrichmentapipersonphoneverificationapihistory).union(personphoneverificationerviceobjectapihistory)
      //print("\nphoneEnrichment"+allPhoneHistory.count())
      //print("\ntest1")
      /*
      filter the union with back date
       */
      allPhoneHistory.createOrReplaceTempView("allPhoneHistory")
      val allPhoneHistoryBackdated = sparkSession.sql("select phone_number from allPhoneHistory where updated_date  >='"+backDate+"'")
      print("allPhoneHistoryBackdated"+allPhoneHistoryBackdated.show())
      val dbPersonallPhone = dbPersonPhone.select("standardised_phone_number")

      /*
      filter the back date records from dbPersonPhone data frame coming from wrapper
       */
      val filteredBackdatedRecords=dbPersonallPhone.except(allPhoneHistoryBackdated)
      //print("\ntest3")
      //print("filteredBackdatedRecords"+filteredBackdatedRecords.show())
      /*
      join back the records to get all columns
      */
      val filtercountrycode=dbPersonPhone.join(filteredBackdatedRecords,"standardised_phone_number")
      print("filtercountrycode"+filtercountrycode.show())
      return filtercountrycode
    }
}
