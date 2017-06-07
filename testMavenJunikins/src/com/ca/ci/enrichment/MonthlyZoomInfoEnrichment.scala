package com.ca.ci.enrichment

import java.util.{Calendar, Properties}
import java.text.SimpleDateFormat
import java.util.Properties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object MonthlyZoomInfoEnrichment {

  val appname = "MonthlyZoomInfoEnrichment"
  val started = "started"
  val ended = "ended"
  /*
  map for config file
  */
  var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty
  /*
  case class for person match records
   */
  case class Records(first_name:String,
                     last_name:String,
                     email_address:String,
                     matchConfidence:String,
                     personId:String,
                     zoomPersonUrl:String,
                     personDetailXmlUrl:String,
                     lastMentioned:String,
                     isUserPosted:String,
                     isPast:String,
                     referencesCount:
                     String,
                     firstName:String,
                     lastName:String,
                     phone:String,
                     fax:String)
  /*
  case class for person details
   */
  case class PersonRecords
  (
    PersonID:String,
    ZoomPersonUrl:String,
    LastUpdatedDate:String,
    IsUserPosted:String,
    ImageUrl:String,
    FirstName:String,
    LastName:String,
    Email:String,
    DirectPhone:String,
    CompanyPhone:String,
    Biography:String
  )

  case class TopLevelIndustryDetails(PersonID:String,TopLevelIndustry:String)

  var TopLevelIndustryList:scala.collection.mutable.ListBuffer[TopLevelIndustryDetails]=scala.collection.mutable.ListBuffer.empty

  case class CertificationsDetails
  (
    PersonID:String,
    CertificationName:String,
    OrganizationName:String,
    YearReceived:String

  )
  var CertificationsList:scala.collection.mutable.ListBuffer[CertificationsDetails]=scala.collection.mutable.ListBuffer.empty

  case class CurrentEmploymentDetails(
                                       CurrentPersonID:String,
                                       CurrentJobTitle:String,
                                       Currentjobfunctiondescription:String,
                                       Currentmanagementleveldescription:String,
                                       CurrentFromDate:String,
                                       CurrentToDate:String,
                                       CurrentCompanyID:String,
                                       CurrentZoomCompanyUrl:String,
                                       CurrentCompanyDetailXmlUrl:String,
                                       CurrentCompanyName:String,
                                       CurrentCompanyPhone:String,
                                       CurrentCompanyFax:String,
                                       CurrentWebsite:String,
                                       CurrentAddressStreet:String,
                                       CurrentAddressCity:String,
                                       CurrentAddressState:String,
                                       CurrentAddressZip:String,
                                       CurrentAddressCountryCode:String)
  var CurrentEmploymentList:scala.collection.mutable.ListBuffer[CurrentEmploymentDetails]=scala.collection.mutable.ListBuffer.empty

  case class PastEmploymentDetails(
                                    PersonID:String,
                                    JobTitle:String,
                                    JobFunction:String,
                                    ManagementLevel:String,
                                    FromDate:String,
                                    ToDate:String,
                                    CompanyID:String,
                                    ZoomCompanyUrl:String,
                                    CompanyDetailXmlUrl:String,
                                    CompanyName:String,
                                    Phone:String,
                                    Fax:String,
                                    Website:String,
                                    AddressStreet:String,
                                    AddressCity:String,
                                    AddressState:String,
                                    Zip:String,
                                    AddressCountryCode:String)

  var PastEmploymentList:scala.collection.mutable.ListBuffer[PastEmploymentDetails]=scala.collection.mutable.ListBuffer.empty


  case class IndustryDetails(PersonID:String,Industry:String)
  /*
 case class for person WebReferenceDetails
  */
  case class WebReferenceDetails(PersonID:String,Title:String,Url:String,Description:String,Date:String)
  /*
case class for person WebReferenceDetails
 */
  case class EducationDetails(PersonID:String,School:String,GraduationDate:String,Degree:String,AreaOfStudy:String)
  /*
   buffer list for person match records
  */
  var RecordsList:scala.collection.mutable.ListBuffer[Records]=scala.collection.mutable.ListBuffer.empty
  /*
  buffer list for person Details records
 */
  var PersonList:scala.collection.mutable.ListBuffer[PersonRecords]=scala.collection.mutable.ListBuffer.empty
  /*
  buffer list for Education Details
 */
  var EducationList:scala.collection.mutable.ListBuffer[EducationDetails]=scala.collection.mutable.ListBuffer.empty
  /*
  buffer list for Web Reference Details
 */
  var WebReference:scala.collection.mutable.ListBuffer[WebReferenceDetails]=scala.collection.mutable.ListBuffer.empty

  var IndustryList:scala.collection.mutable.ListBuffer[IndustryDetails]=scala.collection.mutable.ListBuffer.empty

  //case class PastEmploymentDetails(PersonID:String,JobTitle:String,FromDate:String,ToDate:String,CompanyName:String)

  //var PastEmploymentList:scala.collection.mutable.ListBuffer[PastEmploymentDetails]=scala.collection.mutable.ListBuffer.empty

  var rulekey:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty
  var filePath:String=null
  var myConfigFile:String=null
  /*
  Spark job main method will invoke first
  */
  def main(arg: Array[String]) = {
    println("*********MonthlyZoomInfoEnrichment main method started***********")
    val sparkConf = new SparkConf().setAppName(appname)
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder.appName(appname).getOrCreate()

    filePath=arg(0)
    myConfigFile=arg(1)

    //filePath="wasb://staging@imsshdfsqa.blob.core.windows.net/sfdc/contact/CASparkJobEnrichmentConfig.csv"
    //myConfigFile="wasb://staging@imsshdfsqa.blob.core.windows.net/sfdc/contact/CASparkJobEnrichmentkey.csv"

    //val sparkConf = new SparkConf().setAppName(appname).setMaster("local")
    //val sparkContext = new SparkContext(sparkConf)
    //val sparkSession = SparkSession.builder.appName("Zoom_Info_Enrichment").config("spark.sql.warehouse.dir", "file:///C:/tempwarehouse").getOrCreate()

    /*
    reading all config files values
    */
    ruleMap = readEnrichmentPropFile.getval(sparkSession,filePath)
    //print("ruleMap:" + ruleMap)

    val jdbcURL=ruleMap.get("jdbcURL").get
    val userName=ruleMap.get("userName").get
    val dbUser=ruleMap.get("dbUser").get

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
    val instanceName="Monthly_ZoomInfo_Enrichment_Load"+System.currentTimeMillis()

    val entitydetail = sparkSession.read.jdbc(jdbcURL, "mdm.entity_detail", connectionProperties).select("entity_detail_key").where(" entity_name='ZoomInfo_Person_Match_Temp' ")

    val entity_detail_key=entitydetail.collect().toList(0).get(0).toString
    //print("entity_detail_key: "+entity_detail_key)

    val jobStatusStart = sparkSession.sql("""select distinct """+maxJobKey+""" as batch_id,'"""+instanceName+"""' as instance_name,'started' as status,'"""+file_key+"""' as file_key ,
   '"""+entity_detail_key+"""' as entity_detail_key ,current_timestamp() as created_date,'"""+userName+"""' as created_user
    from jobs_master_table a """)
    //print(jobStatusStart.show())
    jobStatusStart.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)

    /*
    main Zoom info processing method
     */
    processRecords(sparkSession,ruleMap,jdbcURL,connectionProperties)

    /*
     saving spark job start and end time in  spark_jobs_run_details
    */
    val jobStatusEnd = sparkSession.sql("select distinct "+maxJobKey+" as batch_id,'"+instanceName+"'"+" as instance_name,'ended' as status,'"+file_key+"' as file_key , '"+entity_detail_key+"' as entity_detail_key ,current_timestamp() as created_date,'"+userName+"' as created_user from jobs_master_table")
    jobStatusEnd.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)
    val endTime = System.currentTimeMillis()

    //println("Overall Time taken by zoominfo (seconds) :"+(endTime-startTime )/1000)
    sparkSession.stop()
    sparkContext.stop()

  }
  /*
  main processing method
   */
  def processRecords(sparkSession: SparkSession,ruleMap:scala.collection.mutable.Map[String, String],jdbcURL:String,connectionProperties:Properties) {
    println("...inside processRecords zoominfo...")

    val backDateDaysPersonMatch=ruleMap.get("backDateDaysPersonMatch").get
    val backDateDaysPersonDetail=ruleMap.get("backDateDaysPersonDetail").get

    println("backDateDaysPersonDetail:=" + backDateDaysPersonDetail)

    val zoominfoPersonMatchUrl=ruleMap.get("zoominfoPersonMatchUrl").get
    val zoominfoPersonMatchHost=ruleMap.get("zoominfoPersonMatchHost").get
    val zoominfoPersonMatchcanonicaluri=ruleMap.get("zoominfoPersonMatchcanonicaluri").get

    val throttlingFactorPersonMatch =ruleMap.get("throttlingFactorPersonMatch").get
    val timeToWaitInsecondPersonMatch = ruleMap.get("timeToWaitInsecondPersonMatch").get

    println("throttlingFactorPersonMatch:=" + throttlingFactorPersonMatch)
    //println("timeToWaitInsecondPersonMatch:=" + timeToWaitInsecondPersonMatch)

    val zoominfoPersonDetailUrl=ruleMap.get("zoominfoPersonDetailUrl").get
    val zoominfoPersonDetailHost=ruleMap.get("zoominfoPersonDetailHost").get
    val zoominfoPersonDetailcanonicaluri=ruleMap.get("zoominfoPersonDetailcanonicaluri").get

    val throttlingFactorPersonDetail =ruleMap.get("throttlingFactorPersonDetail").get
    val timeToWaitInsecondPersonDetail = ruleMap.get("timeToWaitInsecondPersonDetail").get

    val access_key=ruleMap.get("access_key").get
    val secret_key=ruleMap.get("secret_key").get
    try {
      /*
      Get the 15 days back date from current date
     */
      val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")

      val calpm: Calendar = Calendar.getInstance();
      calpm.add(Calendar.DATE, backDateDaysPersonMatch.toInt)
      val laterdatePm = calpm.getTime
      val backDatePersonMatch = dateFormatter.format(laterdatePm)
      //println("backDatePersonMatch:" + backDatePersonMatch)

      val calPD: Calendar = Calendar.getInstance();
      calPD.add(Calendar.DATE, backDateDaysPersonDetail.toInt)
      val laterdatePD = calPD.getTime
      val backDatePersonDetail = dateFormatter.format(laterdatePD)
      println("backDatePersonDetail:" + backDatePersonDetail)

      /*
      Get the all the records from batch run detail view
       */
      val dbBatchRunDetails = sparkSession.read.jdbc(jdbcURL, "mdm.v_batch_run_details_monthly", connectionProperties)
      /*
      load person table bridge_person_enrichment_api_person
      */
      val dbbridgeperson = sparkSession.read.jdbc(jdbcURL, "mdm.bridge_person_enrichment_api_person", connectionProperties).select("person_key")
      /*
       load person table person_email
      */
      val dbEmail = sparkSession.read.jdbc(jdbcURL, "mdm.person_email", connectionProperties).select("person_key", "email_address")

      /*
      get current month
       */
        val calM:Calendar  = Calendar.getInstance();
        val monthlydate=calM.getTime
        val dateFormattermonthly = new SimpleDateFormat("yyyy-MM")
        val currentMonth = dateFormattermonthly.format(monthlydate)
       /*
      get only current date records from PersonRunDetailDF or person table
       */
      dbBatchRunDetails.createOrReplaceTempView("PersonRunDetailDF_Temp_Table")
      val dbPersonRunfilter = sparkSession.sql("select * from PersonRunDetailDF_Temp_Table where person_updated_date like'" + currentMonth + "%'")
      //print("dbPersonRunfilter"+dbPersonRunfilter.count())

      /*
      remove all records which are present in bridge_person_enrichment_api_person table
       */
      val dbPersonPersonKey =dbPersonRunfilter.select("person_key")
      val filterfromdbbridgeperson= dbPersonPersonKey.except(dbbridgeperson)
      //println("filtered data frame from backdate"+filterfromdbbridgeperson.show())
      /*
      join back records to get all columns
      */
      val allPersonFilter=dbPersonRunfilter.join(filterfromdbbridgeperson,"person_key")

      /*
      join with email table to get email address
       */
      val recordToFire = allPersonFilter.join(dbEmail, "person_key")
      //println("***************recordToFire********before distinct"+recordToFire.show())

      recordToFire.createOrReplaceTempView("recordToFireZoomInfoTemp")

      /*
      filter Duplicated records
       */
      val finalDF=sparkSession.sql("select distinct first_name,last_name,email_address from recordToFireZoomInfoTemp")
      //println("Distinct number of records to process:"+finalDF.count())
      /*
      company name or email address should be present in query param
       */
      /*
     call the aws api person detail for each first name last name and email
      */
      //print(access_key,secret_key,zoominfoPersonMatchUrl,zoominfoPersonMatchHost,zoominfoPersonMatchcanonicaluri,throttlingFactorPersonMatch,timeToWaitInsecondPersonMatch)
      val recordToFireList = finalDF.toDF().collect()
      print("Numner of records to fire match" + finalDF.count())

      invokeAPIWithThrottling(recordToFireList.take(5),access_key,secret_key,zoominfoPersonMatchUrl,zoominfoPersonMatchHost,zoominfoPersonMatchcanonicaluri,throttlingFactorPersonMatch,timeToWaitInsecondPersonMatch)

      import sparkSession.implicits._
      val recordsDF = RecordsList.toDF()
      //println(recordsDF.show())
      /*
      get all records from enrichment_api_person
      */
      val enrichmentapiperson = sparkSession.read.jdbc(jdbcURL, "mdm.enrichment_api_person", connectionProperties).select("person_id", "updated_date")

      /*
      Filter the records from enrichment_api_person table with last 15 days updated
       */
      enrichmentapiperson.createOrReplaceTempView("enrichmentapiperson")
      val dbemailapifilter = sparkSession.sql("select person_id from enrichmentapiperson where updated_date  >='" + backDatePersonDetail + "'")
      //print("dbPhoneverification" + dbemailapifilter.show())

      /*
      Filter the records from filerRecord data with records were updated in last 15 days
      */
      val filterRecord = recordsDF.select("PersonID")
      val finalresult = filterRecord.except(dbemailapifilter)

      val filterNullDF=finalresult.filter(finalresult("PersonID").isNotNull)
      print("Numner of records to fire detail" +filterNullDF.count())

      //print("Numner of records to fire detail" + finalresult.count())
      /*
      call the personDetail aws api for each personid
      */
      val finalresultList = filterNullDF.toDF().collect()
      invokeAPIWithThrottlingPersonDetail(finalresultList.take(5),access_key,secret_key,zoominfoPersonDetailUrl,zoominfoPersonDetailHost,zoominfoPersonDetailcanonicaluri,throttlingFactorPersonDetail,timeToWaitInsecondPersonDetail)
    }catch
      {
        case e: Exception => {
          print("Exception handled in processRecords method:"+e.toString()+e.printStackTrace())

        }
      }
    finally {
      //val startTime = System.currentTimeMillis()
      /*
      finally store the data in temp table
       */
      if(!RecordsList.isEmpty) {
        import sparkSession.implicits._
        val recordsDFfinal = RecordsList.toDF()
        recordsDFfinal.createOrReplaceTempView("recordsZoomInfoDFfinalAws")
        val personMatchDF = sparkSession.sql("select distinct a.*,b.person_key from recordsZoomInfoDFfinalAws a,recordToFireZoomInfoTemp b where a.first_name = b.first_name and a.last_name=b.last_name and a.email_address=b.email_address")
        personMatchDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.ZoomInfo_Person_Match_Temp", connectionProperties)
      }
      /*
      finally store the data in temp table
       */
      if(!PersonList.isEmpty) {
        import sparkSession.implicits._
        val PersonListDf = PersonList.toDF()
       //println("PersonDetailListDf" + PersonListDf.show())
        PersonListDf.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.ZoomInfo_Person_Detail_Temp", connectionProperties)
      }

      if(!WebReference.isEmpty) {
        import sparkSession.implicits._
        val personmatchWebReference = WebReference.toDF()
        //println("personmatchWebReference" + personmatchWebReference.show())
        personmatchWebReference.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.person_Detail_WebReference_Temp", connectionProperties)
      }


      if(!CertificationsList.isEmpty) {
        import sparkSession.implicits._
        val CertificationsListDF = CertificationsList.toDF()
        //println("CertificationsListDF" + CertificationsListDF.show())
        CertificationsListDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.person_Detail_Certifications_Temp", connectionProperties)
      }

      if(!EducationList.isEmpty) {
        import sparkSession.implicits._
        val EducationListDF = EducationList.toDF()
        //println("personEducationList" + EducationListDF.show())
        EducationListDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.Person_Detail_Education_Temp", connectionProperties)
      }
      if(!IndustryList.isEmpty) {
        import sparkSession.implicits._
        val IndustryListDF = IndustryList.toDF()
        //println("IndustryListDF" + IndustryListDF.show())
        IndustryListDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.Person_Detail_Industry_Temp", connectionProperties)
      }

      if(!TopLevelIndustryList.isEmpty) {
        import sparkSession.implicits._
        val TopLevelIndustryListDF = TopLevelIndustryList.toDF()
        //println("TopLevelIndustryListDF" + TopLevelIndustryListDF.show())
        TopLevelIndustryListDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.Person_Detail_TopLevelIndustry_Temp", connectionProperties)
      }

      if(!PastEmploymentList.isEmpty) {
        import sparkSession.implicits._
        val PastEmploymentListtDF = PastEmploymentList.toDF()
        //println("PastEmploymentListtDF" + PastEmploymentListtDF.show())
        PastEmploymentListtDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.Person_Detail_Past_Employment_temp", connectionProperties)
      }
      if(!CurrentEmploymentList.isEmpty) {
        import sparkSession.implicits._
        val CurrentEmploymentListDF = CurrentEmploymentList.toDF()
        //println("CurrentEmploymentListDF" + CurrentEmploymentListDF.show())
        CurrentEmploymentListDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.Person_Detail_Current_Employment_temp", connectionProperties)
      }
    }
  }



  /*
throttling algo mehod
 */
  def invokeAPIWithThrottling(dataArray:Array[org.apache.spark.sql.Row],access_key:String,secret_key:String,zoominfoPersonMatchUrl:String,zoominfoPersonMatchHost:String,zoominfoPersonMatchcanonicaluri:String,throttlingFactorPersonMatch:String,timeToWaitInsecondPersonMatch:String)=
  {
    import scala.collection.parallel._
    val dataList = dataArray.toList
    if(!dataList.isEmpty)
    {
      val parallelList =dataList.par
      parallelList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(throttlingFactorPersonMatch.toInt))
      parallelList.map(x=>invokeAPI(x,access_key,secret_key,zoominfoPersonMatchUrl,zoominfoPersonMatchHost,zoominfoPersonMatchcanonicaluri))
    }
  }

    /*
    throttling algo mehod
    */
  def invokeAPIWithThrottlingPersonDetail(dataArray:Array[org.apache.spark.sql.Row],access_key:String,secret_key:String,zoominfoPersonDetailUrl:String,zoominfoPersonDetailHost:String,zoominfoPersonDetailcanonicaluri:String,throttlingFactorPersonDetail:String,timeToWaitInsecondPersonDetail:String)=
  {
    import scala.collection.parallel._
    val dataList = dataArray.toList
    if(!dataList.isEmpty)
    {
      val parallelList =dataList.par
      parallelList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(throttlingFactorPersonDetail.toInt))
      parallelList.map(x=>invokeAPIPersonDetail(x,access_key,secret_key,zoominfoPersonDetailUrl,zoominfoPersonDetailHost,zoominfoPersonDetailcanonicaluri))
    }
  }

  /*
  invokeAPI method will call the aws api for person match
   */
  def invokeAPI(row:org.apache.spark.sql.Row,access_key:String,secret_key:String,zoominfoPersonMatchUrl:String,zoominfoPersonMatchHost:String,zoominfoPersonMatchcanonicaluri:String)
  {
    zoomPersonMatch(row.getAs("first_name"),row.getAs("last_name"),row.getAs("email_address"),com.ca.ci.enrichment.AWSRequestSigner.zoomApiPersonMatch(access_key,secret_key,zoominfoPersonMatchUrl,zoominfoPersonMatchHost,zoominfoPersonMatchcanonicaluri,row.getAs("first_name"),row.getAs("last_name"),row.getAs("email_address")))
  }

  /*
 invokeAPI method will call the aws api person detail
  */
  def invokeAPIPersonDetail(row:org.apache.spark.sql.Row,access_key:String,secret_key:String,zoominfoPersonDetailUrl:String,zoominfoPersonDetailHost:String,zoominfoPersonDetailcanonicaluri:String)
  {
   zoomPersonDetail(row.getAs("PersonID"),com.ca.ci.enrichment.AWSRequestSigner.zoomApiPersonDetail(access_key,secret_key,zoominfoPersonDetailUrl,zoominfoPersonDetailHost,zoominfoPersonDetailcanonicaluri,row.getAs("PersonID")))
  }

  /*
 parser method to process PersonDetail json
  */
  def zoomPersonDetail(person_id:String,response:String) = {
    try{
      if (response != null) {
        if (response.indexOf("ErrorMessage") == -1) {
          val mapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)
          val resultPerson = mapper.readTree(response)
          //print("resultPerson\n" + response)
          var PersonID: String = null
          var ZoomPersonUrl: String = null
          var LastUpdatedDate: String = null
          var IsUserPosted: String = null
          var ImageUrl: String = null
          var FirstName: String = null
          var LastName: String = null
          var Email: String = null
          var DirectPhone: String = null
          var CompanyPhone: String = null
          var Biography: String = null

          if (resultPerson.has("PersonDetailRequest")) {

            if (resultPerson.get("PersonDetailRequest").has("PersonID")) {
              PersonID = resultPerson.get("PersonDetailRequest").get("PersonID").asText()
            }
            if (resultPerson.get("PersonDetailRequest").has("ZoomPersonUrl")) {
              ZoomPersonUrl = resultPerson.get("PersonDetailRequest").get("ZoomPersonUrl").asText()
            }
            if (resultPerson.get("PersonDetailRequest").has("LastUpdatedDate")) {
              LastUpdatedDate = resultPerson.get("PersonDetailRequest").get("LastUpdatedDate").asText()
            }
            if (resultPerson.get("PersonDetailRequest").has("IsUserPosted")) {
              IsUserPosted = resultPerson.get("PersonDetailRequest").get("IsUserPosted").asText()
            }
            if (resultPerson.get("PersonDetailRequest").has("ImageUrl")) {
              ImageUrl = resultPerson.get("PersonDetailRequest").get("ImageUrl").asText()
            }
            if (resultPerson.get("PersonDetailRequest").has("FirstName")) {
              FirstName = resultPerson.get("PersonDetailRequest").get("FirstName").asText()
            }
            if (resultPerson.get("PersonDetailRequest").has("LastName")) {
              LastName = resultPerson.get("PersonDetailRequest").get("LastName").asText()
            }
            if (resultPerson.get("PersonDetailRequest").has("Email")) {
              Email = resultPerson.get("PersonDetailRequest").get("Email").asText()
            }
            if (resultPerson.get("PersonDetailRequest").has("DirectPhone")) {
              DirectPhone = resultPerson.get("PersonDetailRequest").get("DirectPhone").asText()
            }
            if (resultPerson.get("PersonDetailRequest").has("CompanyPhone")) {
              CompanyPhone = resultPerson.get("PersonDetailRequest").get("CompanyPhone").asText()
            }
            if (resultPerson.get("PersonDetailRequest").has("Biography")) {
              Biography = resultPerson.get("PersonDetailRequest").get("Biography").asText()
            }


            var CurrentJobTitle: String = null
            var Currentjobfunctiondescription: String = null
            var Currentmanagementleveldescription:String = null
            var CurrentFromDate: String = null
            var CurrentToDate: String = null
            var CurrentCompanyID: String = null
            var CurrentZoomCompanyUrl: String = null
            var CurrentCompanyDetailXmlUrl: String = null
            var CurrentCompanyName: String = null
            var CurrentCompanyPhone: String = null
            var CurrentCompanyFax: String = null
            var CurrentWebsite: String = null
            var CurrentAddressStreet: String = null
            var CurrentAddressCity: String = null
            var CurrentAddressState: String = null
            var CurrentAddressZip : String = null
            var CurrentAddressCountryCode: String = null

            if (resultPerson.get("PersonDetailRequest").has("CurrentEmployment")) {
              if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").isArray) {
                val itrce = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").iterator()
                var itrcount = 0
                while (itrce.hasNext) {
                  CurrentJobTitle = null
                  Currentjobfunctiondescription = null
                  Currentmanagementleveldescription = null
                  CurrentFromDate = null
                  CurrentToDate = null
                  CurrentCompanyID = null
                  CurrentZoomCompanyUrl = null
                  CurrentCompanyDetailXmlUrl = null
                  CurrentCompanyName = null
                  CurrentCompanyPhone = null
                  CurrentCompanyFax = null
                  CurrentWebsite = null
                  CurrentAddressStreet = null
                  CurrentAddressCity = null
                  CurrentAddressState = null
                  CurrentAddressZip = null
                  CurrentAddressCountryCode = null

                  if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").has(itrcount)) {
                    if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).has("JobTitle")) {
                      CurrentJobTitle = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("JobTitle").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).has("JobFunction")) {
                      Currentjobfunctiondescription = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("JobFunction").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).has("ManagementLevel")) {
                      Currentmanagementleveldescription = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("ManagementLevel").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).has("FromDate")) {
                      CurrentFromDate = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("FromDate").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).has("ToDate")) {
                      CurrentToDate = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("ToDate").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).has("Company")) {
                      if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").has("CompanyID")) {
                        CurrentCompanyID = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyID").asText()
                      }
                      if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").has("ZoomCompanyUrl")) {
                        CurrentZoomCompanyUrl = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("ZoomCompanyUrl").asText()
                      }
                      if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").has("CompanyDetailXmlUrl")) {
                        CurrentCompanyDetailXmlUrl = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyDetailXmlUrl").asText()
                      }
                      if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").has("CompanyName")) {
                        CurrentCompanyName = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyName").asText()
                      }

                      if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").has("Phone")) {
                        CurrentCompanyPhone = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("Phone").asText()
                      }
                      if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").has("Fax")) {
                        CurrentCompanyFax = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("Fax").asText()
                      }

                      if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").has("Website")) {
                        CurrentWebsite = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("Website").asText()
                      }
                      if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").has("CompanyAddress")) {
                        if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyAddress").has("Street")) {
                          CurrentAddressStreet = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyAddress").get("Street").asText()
                        }
                        if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyAddress").has("City")) {
                          CurrentAddressCity = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyAddress").get("City").asText()
                        }
                        if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyAddress").has("State")) {
                          CurrentAddressState = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyAddress").get("State").asText()
                        }
                        if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyAddress").has("Zip")) {
                          CurrentAddressZip = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyAddress").get("Zip").asText()
                        }
                        if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyAddress").has("CountryCode")) {
                          CurrentAddressCountryCode = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get(itrcount).get("Company").get("CompanyAddress").get("CountryCode").asText()
                        }
                      }
                    }
                    CurrentEmploymentList.append(
                      CurrentEmploymentDetails(
                        PersonID,
                        CurrentJobTitle,
                        Currentjobfunctiondescription,
                        Currentmanagementleveldescription,
                        CurrentFromDate,
                        CurrentToDate,
                        CurrentCompanyID,
                        CurrentZoomCompanyUrl,
                        CurrentCompanyDetailXmlUrl,
                        CurrentCompanyName,
                        CurrentCompanyPhone,
                        CurrentCompanyFax,
                        CurrentWebsite,
                        CurrentAddressStreet,
                        CurrentAddressCity,
                        CurrentAddressState,
                        CurrentAddressZip,
                        CurrentAddressCountryCode))
                    itrce.next()
                    itrcount = itrcount + 1
                  }
                }
              } else {

                if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").has("JobTitle")) {
                  CurrentJobTitle = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("JobTitle").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").has("JobFunction")) {
                  Currentjobfunctiondescription = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("JobFunction").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").has("ManagementLevel")) {
                  Currentmanagementleveldescription = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("ManagementLevel").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").has("FromDate")) {
                  CurrentFromDate = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("FromDate").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").has("ToDate")) {
                  CurrentToDate = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("ToDate").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").has("Company")) {
                  if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").has("CompanyID")) {
                    CurrentCompanyID = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyID").asText()
                  }
                  if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").has("ZoomCompanyUrl")) {
                    CurrentZoomCompanyUrl = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("ZoomCompanyUrl").asText()
                  }
                  if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").has("CompanyDetailXmlUrl")) {
                    CurrentCompanyDetailXmlUrl = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyDetailXmlUrl").asText()
                  }
                  if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").has("CompanyName")) {
                    CurrentCompanyName = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyName").asText()
                  }

                  if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").has("Phone")) {
                    CurrentCompanyPhone = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("Phone").asText()
                  }
                  if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").has("Fax")) {
                    CurrentCompanyFax = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("Fax").asText()
                  }

                  if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").has("Website")) {
                    CurrentWebsite = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("Website").asText()
                  }
                  if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").has("CompanyAddress")) {
                    if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyAddress").has("Street")) {
                      CurrentAddressStreet = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyAddress").get("Street").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyAddress").has("City")) {
                      CurrentAddressCity = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyAddress").get("City").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyAddress").has("State")) {
                      CurrentAddressState = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyAddress").get("State").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyAddress").has("Zip")) {
                      CurrentAddressZip = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyAddress").get("Zip").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyAddress").has("CountryCode")) {
                      CurrentAddressCountryCode = resultPerson.get("PersonDetailRequest").get("CurrentEmployment").get("Company").get("CompanyAddress").get("CountryCode").asText()
                    }
                  }
                }
                CurrentEmploymentList.append(
                  CurrentEmploymentDetails(
                    PersonID,
                    CurrentJobTitle,
                    Currentjobfunctiondescription,
                    Currentmanagementleveldescription,
                    CurrentFromDate,
                    CurrentToDate,
                    CurrentCompanyID,
                    CurrentZoomCompanyUrl,
                    CurrentCompanyDetailXmlUrl,
                    CurrentCompanyName,
                    CurrentCompanyPhone,
                    CurrentCompanyFax,
                    CurrentWebsite,
                    CurrentAddressStreet,
                    CurrentAddressCity,
                    CurrentAddressState,
                    CurrentAddressZip,
                    CurrentAddressCountryCode))

              }
            }
            /*
          Getting the fileds for webreference and store in web referencelist
            */
            var Title: String = null
            var weburl: String = null
            var Description: String = null
            var Date: String = null
            if (resultPerson.get("PersonDetailRequest").has("WebReference")) {
              if (resultPerson.get("PersonDetailRequest").get("WebReference").isArray) {
                val itrweb = resultPerson.get("PersonDetailRequest").get("WebReference").iterator()
                var count = 0
                while (itrweb.hasNext) {
                  Title = null
                  weburl = null
                  Description = null
                  Date = null
                  if (resultPerson.get("PersonDetailRequest").get("WebReference").has(count)) {
                    if (resultPerson.get("PersonDetailRequest").get("WebReference").get(count).has("Title")) {
                      Title = resultPerson.get("PersonDetailRequest").get("WebReference").get(count).get("Title").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("WebReference").get(count).has("Url")) {
                      weburl = resultPerson.get("PersonDetailRequest").get("WebReference").get(count).get("Url").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("WebReference").get(count).has("Description")) {
                      Description = resultPerson.get("PersonDetailRequest").get("WebReference").get(count).get("Description").asText()
                      if(Description.toString.length>=500)
                      {
                        Description=Description.toString.substring(0,498)
                      }
                    }
                    if (resultPerson.get("PersonDetailRequest").get("WebReference").get(count).has("Date")) {
                      Date = resultPerson.get("PersonDetailRequest").get("WebReference").get(count).get("Date").asText()
                    }
                    WebReference.append(WebReferenceDetails(PersonID, Title, weburl, Description, Date))
                    itrweb.next()
                    count = count + 1
                  }
                }
              } else {
                if (resultPerson.get("PersonDetailRequest").get("WebReference").has("Title")) {
                  Title = resultPerson.get("PersonDetailRequest").get("WebReference").get("Title").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("WebReference").has("Url")) {
                  weburl = resultPerson.get("PersonDetailRequest").get("WebReference").get("Url").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("WebReference").has("Description")) {
                  Description = resultPerson.get("PersonDetailRequest").get("WebReference").get("Description").asText()
                  if(Description.toString.length>=500)
                  {
                    Description=Description.toString.substring(0,498)
                  }
                }
                if (resultPerson.get("PersonDetailRequest").get("WebReference").has("Date")) {
                  Date = resultPerson.get("PersonDetailRequest").get("WebReference").get("Date").asText()
                }
                WebReference.append(WebReferenceDetails(PersonID, Title, weburl, Description, Date))
              }
            }
            /*
        getting the values for education and store in the list
         */
            var School: String = null
            var GraduationDate: String = null
            var Degree: String = null
            var AreaOfStudy: String = null
            if (resultPerson.get("PersonDetailRequest").has("Education")) {
              if (resultPerson.get("PersonDetailRequest").get("Education").isArray) {
                val itredu = resultPerson.get("PersonDetailRequest").get("Education").iterator()
                var count = 0
                while (itredu.hasNext) {
                  School = null
                  GraduationDate = null
                  Degree = null
                  AreaOfStudy = null
                  if (resultPerson.get("PersonDetailRequest").get("Education").has(count)) {
                    if (resultPerson.get("PersonDetailRequest").get("Education").get(count).has("School")) {
                      School = resultPerson.get("PersonDetailRequest").get("Education").get(count).get("School").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("Education").get(count).has("GraduationDate")) {
                      GraduationDate = resultPerson.get("PersonDetailRequest").get("Education").get(count).get("GraduationDate").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("Education").get(count).has("EducationDegree")) {
                      if (resultPerson.get("PersonDetailRequest").get("Education").get(count).get("EducationDegree").has("Degree")) {
                        Degree = resultPerson.get("PersonDetailRequest").get("Education").get(count).get("EducationDegree").get("Degree").asText()
                      }
                      if (resultPerson.get("PersonDetailRequest").get("Education").get(count).get("EducationDegree").has("AreaOfStudy")) {
                        AreaOfStudy = resultPerson.get("PersonDetailRequest").get("Education").get(count).get("EducationDegree").get("AreaOfStudy").asText()
                      }
                    }
                    EducationList.append(EducationDetails(PersonID, School, GraduationDate, Degree, AreaOfStudy))
                    itredu.next()
                    count = count + 1
                  }
                }
              } else {
                if (resultPerson.get("PersonDetailRequest").get("Education").has("School")) {
                  School = resultPerson.get("PersonDetailRequest").get("Education").get("School").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("Education").has("GraduationDate")) {
                  GraduationDate = resultPerson.get("PersonDetailRequest").get("Education").get("GraduationDate").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("Education").has("EducationDegree")) {
                  if (resultPerson.get("PersonDetailRequest").get("Education").get("EducationDegree").has("Degree")) {
                    Degree = resultPerson.get("PersonDetailRequest").get("Education").get("EducationDegree").get("Degree").asText()
                  }
                  if (resultPerson.get("PersonDetailRequest").get("Education").get("EducationDegree").has("AreaOfStudy")) {
                    AreaOfStudy = resultPerson.get("PersonDetailRequest").get("Education").get("EducationDegree").get("AreaOfStudy").asText()
                  }
                }
                EducationList.append(EducationDetails(PersonID, School, GraduationDate, Degree, AreaOfStudy))
              }
            }


            /*
            getting the values for Past Employment and store in the list
            */
            var JobTitle: String = null
            var JobFunction: String = null
            var ManagementLevel: String = null
            var FromDate: String = null
            var ToDate: String = null
            var CompanyName: String = null
            var CompanyID: String = null
            var ZoomCompanyUrl: String = null
            var CompanyDetailXmlUrl: String = null
            var Website: String = null
            var Phone: String = null
            var Fax: String = null

            var AddressStreet : String = null
            var AddressCity: String = null
            var AddressState: String = null
            var AddressZip: String = null
            var AddressCountryCode: String = null


            if (resultPerson.get("PersonDetailRequest").has("PastEmployment")) {
              if (resultPerson.get("PersonDetailRequest").get("PastEmployment").isArray) {
                val itrpemp = resultPerson.get("PersonDetailRequest").get("PastEmployment").iterator()
                var count = 0
                while (itrpemp.hasNext) {
                  JobTitle = null
                  JobFunction = null
                  ManagementLevel = null
                  FromDate = null
                  ToDate = null
                  CompanyName = null
                  CompanyID = null
                  ZoomCompanyUrl = null
                  CompanyDetailXmlUrl = null
                  Website = null
                  AddressStreet = null
                  AddressCity= null
                  AddressState= null
                  AddressCountryCode= null
                  Phone=null
                  Fax=null
                  AddressZip=null
                  if (resultPerson.get("PersonDetailRequest").get("PastEmployment").has(count)) {
                    if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).has("JobTitle")) {
                      JobTitle = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("JobTitle").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).has("JobFunction")) {
                      JobFunction = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("JobFunction").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).has("ManagementLevel")) {
                      ManagementLevel = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("ManagementLevel").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).has("FromDate")) {
                      FromDate = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("FromDate").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).has("ToDate")) {
                      ToDate = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("ToDate").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).has("Company")) {


                      if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").has("CompanyID")) {
                        CompanyID = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyID").asText()
                      }
                      if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").has("ZoomCompanyUrl")) {
                        ZoomCompanyUrl = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("ZoomCompanyUrl").asText()
                      }
                      if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").has("CompanyDetailXmlUrl")) {
                        CompanyDetailXmlUrl = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyDetailXmlUrl").asText()
                      }
                      if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").has("CompanyName")) {
                        CompanyName = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyName").asText()
                      }
                      if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").has("Phone")) {
                        Phone = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("Phone").asText()
                      }
                      if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").has("Fax")) {
                        Fax = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("Fax").asText()
                      }

                      if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").has("Website")) {
                        Website = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("Website").asText()
                      }

                      if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").has("CompanyAddress")) {
                        if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyAddress").has("Street")) {
                          AddressStreet = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyAddress").get("Street").asText()
                        }
                        if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyAddress").has("City")) {
                          AddressCity = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyAddress").get("City").asText()
                        }
                        if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyAddress").has("State")) {
                          AddressState = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyAddress").get("State").asText()
                        }
                        if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyAddress").has("Zip")) {
                          AddressZip = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyAddress").get("Zip").asText()
                        }
                        if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyAddress").has("CountryCode")) {
                          AddressCountryCode = resultPerson.get("PersonDetailRequest").get("PastEmployment").get(count).get("Company").get("CompanyAddress").get("CountryCode").asText()
                        }
                      }
                    }

                    PastEmploymentList.append(PastEmploymentDetails(
                      PersonID,
                      JobTitle,
                      JobFunction,
                      ManagementLevel,
                      FromDate,
                      ToDate,
                      CompanyID,
                      ZoomCompanyUrl,
                      CompanyDetailXmlUrl,
                      CompanyName,
                      Phone,
                      Fax,
                      Website,
                      AddressStreet,
                      AddressCity,
                      AddressState,
                      AddressZip,
                      AddressCountryCode))
                      itrpemp.next()
                      count = count + 1
                  }
                }
              } else {
                if (resultPerson.get("PersonDetailRequest").get("PastEmployment").has("JobTitle")) {
                  JobTitle = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("JobTitle").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("PastEmployment").has("JobFunction")) {
                  JobFunction = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("JobFunction").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("PastEmployment").has("ManagementLevel")) {
                  ManagementLevel = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("ManagementLevel").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("PastEmployment").has("FromDate")) {
                  FromDate = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("FromDate").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("PastEmployment").has("ToDate")) {
                  ToDate = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("ToDate").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("PastEmployment").has("Company")) {

                  if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").has("CompanyID")) {
                    CompanyID = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyID").asText()
                  }
                  if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").has("ZoomCompanyUrl")) {
                    ZoomCompanyUrl = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("ZoomCompanyUrl").asText()
                  }
                  if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").has("CompanyDetailXmlUrl")) {
                    CompanyDetailXmlUrl = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyDetailXmlUrl").asText()
                  }
                  if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").has("CompanyName")) {
                    CompanyName = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyName").asText()
                  }

                  if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").has("Phone")) {
                    Phone = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("Phone").asText()
                  }
                  if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").has("Fax")) {
                    Fax = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("Fax").asText()
                  }

                  if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").has("Website")) {
                    Website = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("Website").asText()
                  }
                  if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").has("CompanyAddress")) {
                    if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyAddress").has("Street")) {
                      AddressStreet = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyAddress").get("Street").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyAddress").has("City")) {
                      AddressCity = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyAddress").get("City").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyAddress").has("State")) {
                      AddressState = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyAddress").get("State").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyAddress").has("Zip")) {
                      AddressZip = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyAddress").get("Zip").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyAddress").has("CountryCode")) {
                      AddressCountryCode = resultPerson.get("PersonDetailRequest").get("PastEmployment").get("Company").get("CompanyAddress").get("CountryCode").asText()
                    }
                  }

                }
                PastEmploymentList.append(PastEmploymentDetails(
                  PersonID,
                  JobTitle,
                  JobFunction,
                  ManagementLevel,
                  FromDate,
                  ToDate,
                  CompanyID,
                  ZoomCompanyUrl,
                  CompanyDetailXmlUrl,
                  CompanyName,
                  Phone,
                  Fax,
                  Website,
                  AddressStreet,
                  AddressCity,
                  AddressState,
                  AddressZip,
                  AddressCountryCode)) }
            }

            var Industry: String = null
            if (resultPerson.get("PersonDetailRequest").has("Industry")) {
              val itrIndustry = resultPerson.get("PersonDetailRequest").get("Industry").iterator()
              var count = 0
              while (itrIndustry.hasNext) {
                Industry = null
                if (resultPerson.get("PersonDetailRequest").get("Industry").has(count)) {
                  Industry = resultPerson.get("PersonDetailRequest").get("Industry").get(count).asText()
                }
                IndustryList.append(IndustryDetails(PersonID, Industry))
                itrIndustry.next()
                count = count + 1
              }
            }

            var TopLevelIndustry: String = null
            if (resultPerson.get("PersonDetailRequest").has("TopLevelIndustry")) {
              val itrtopIndustry = resultPerson.get("PersonDetailRequest").get("TopLevelIndustry").iterator()
              var count = 0
              while (itrtopIndustry.hasNext) {
                TopLevelIndustry = null
                if (resultPerson.get("PersonDetailRequest").get("TopLevelIndustry").has(count)) {
                  TopLevelIndustry = resultPerson.get("PersonDetailRequest").get("TopLevelIndustry").get(count).asText()
                }
                TopLevelIndustryList.append(TopLevelIndustryDetails(PersonID, TopLevelIndustry))
                itrtopIndustry.next()
                count = count + 1
              }
            }



            var CertificationName: String = null
            var OrganizationName: String = null
            var YearReceived: String = null
            if (resultPerson.get("PersonDetailRequest").has("Certifications"))
            {
              if (resultPerson.get("PersonDetailRequest").get("Certifications").isArray) {
                val itredu = resultPerson.get("PersonDetailRequest").get("Certifications").iterator()
                var count = 0
                while (itredu.hasNext) {
                  CertificationName = null
                  OrganizationName = null
                  YearReceived = null

                  if (resultPerson.get("PersonDetailRequest").get("Certifications").has(count)) {
                    if (resultPerson.get("PersonDetailRequest").get("Certifications").get(count).has("CertificationName")) {
                      CertificationName = resultPerson.get("PersonDetailRequest").get("Certifications").get(count).get("CertificationName").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("Certifications").get(count).has("OrganizationName")) {
                      OrganizationName = resultPerson.get("PersonDetailRequest").get("Certifications").get(count).get("OrganizationName").asText()
                    }
                    if (resultPerson.get("PersonDetailRequest").get("Certifications").get(count).has("YearReceived")) {
                      YearReceived = resultPerson.get("PersonDetailRequest").get("Certifications").get(count).get("YearReceived").asText()
                    }
                    CertificationsList.append(CertificationsDetails(PersonID, CertificationName, OrganizationName, YearReceived))
                    itredu.next()
                    count = count + 1
                  }
                }
              } else {
                if (resultPerson.get("PersonDetailRequest").get("Certifications").has("CertificationName")) {
                  CertificationName = resultPerson.get("PersonDetailRequest").get("Certifications").get("CertificationName").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("Certifications").has("OrganizationName")) {
                  OrganizationName = resultPerson.get("PersonDetailRequest").get("Certifications").get("OrganizationName").asText()
                }
                if (resultPerson.get("PersonDetailRequest").get("Certifications").has("YearReceived")) {
                  YearReceived = resultPerson.get("PersonDetailRequest").get("Certifications").get("YearReceived").asText()
                }
                CertificationsList.append(CertificationsDetails(PersonID, CertificationName, OrganizationName, YearReceived))
              }
            }

            if (!PersonID.equals("")) {
              PersonList.append(
                PersonRecords(
                  PersonID,
                  ZoomPersonUrl,
                  LastUpdatedDate,
                  IsUserPosted,
                  ImageUrl,
                  FirstName,
                  LastName,
                  Email,
                  DirectPhone,
                  CompanyPhone,
                  Biography))
            }
          }
        }else{
          println("ERROR: Exception handled zoomPersonDetail person_id:"+person_id+"\nresponse"+response)
        }
      }
    }catch {
      case e: Exception => {
        println("ERROR: Exception handled in zoomPersonDetail person_id:"+person_id+"\nresponse"+response+"\nerror message"+e.toString()+e.printStackTrace())

      }
    }
  }

  /*
parser method to process person match json
*/
  def zoomPersonMatch(first_name:String,last_name:String,email_address:String,response: String) = {
    try {
    if (response != null) {
      if (response.indexOf("ErrorMessage") == -1) {
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        val result = mapper.readTree(response)

        var matchConfidence: String = null
        var phone: String = null
        var personId: String = null
        var zoomPersonUrl: String = null
        var personDetailXmlUrl: String = null
        var lastMentioned: String = null
        var isUserPosted: String = null
        var isPast: String = null
        var referencesCount: String = null
        var firstName: String = null
        var lastName: String = null
        var fax: String = null


        if (result.has("personMatchResponse")) {
          if (result.get("personMatchResponse").has("matchResults")) {
            if (result.get("personMatchResponse").get("matchResults").has("personMatchResults")) {
              if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").has("personMatchResult")) {
                if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").has("personMatches")) {
                  if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").has("personMatch")) {

                    if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("matchConfidence")) {
                      matchConfidence = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("matchConfidence").asText()

                      if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("personId")) {
                        personId = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("personId").asText()
                      }
                      if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("zoomPersonUrl")) {
                        zoomPersonUrl = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("zoomPersonUrl").asText()
                      }
                      if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("personDetailXmlUrl")) {
                        personDetailXmlUrl = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("personDetailXmlUrl").asText()
                      }
                      if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("lastMentioned")) {
                        lastMentioned = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("lastMentioned").asText()
                      }
                      if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("isUserPosted")) {
                        isUserPosted = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("isUserPosted").asText()
                      }
                      if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("isPast")) {
                        isPast = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("isPast").asText()
                      }
                      if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("referencesCount")) {
                        referencesCount = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("referencesCount").asText()
                      }
                      if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("firstName")) {
                        firstName = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("firstName").asText()
                      }
                      if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("lastName")) {
                        lastName = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("lastName").asText()
                      }
                      if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("phone")) {
                        phone = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("phone").asText()
                      }
                      if (result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").has("fax")) {
                        fax = result.get("personMatchResponse").get("matchResults").get("personMatchResults").get("personMatchResult").get("personMatches").get("personMatch").get("fax").asText()
                      }
                          RecordsList.append(Records(
                          first_name,
                          last_name,
                          email_address,
                          matchConfidence,
                          personId,
                          zoomPersonUrl,
                          personDetailXmlUrl,
                          lastMentioned,
                          isUserPosted,
                          isPast,
                          referencesCount,
                          firstName,
                          lastName,
                          phone,
                          fax))
                    }
                  }
                }
              }
            }
          }
        }
      }else{
        println("ERROR: Exception handled zoomPersonMatch response:"+response+"\nfirst_name:"+first_name+"\nlast_name"+last_name+"\nemail_address"+email_address)
      }
    }
    }catch {
        case e: Exception => {
         println("ERROR:Exception handled zoomPersonMatch response:"+response+"\nfirst_name:"+first_name+"\nlast_name"+last_name+"\nemail_address"+email_address+"\nerror message"+e.toString()+e.printStackTrace())
          }
      }
  }
}