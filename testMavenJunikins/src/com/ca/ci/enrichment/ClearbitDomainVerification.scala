package com.ca.ci.enrichment

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by punma03 on 3/7/2017.
  */
object ClearbitDomainVerification {
  /*
  map for property file
   */
  var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty
    /*
    case class to store fields comming from aws api
     */
  case class DomainDetails(id:String,
                           name:String,
                           legalName:String,
                           domain:String,
                           url:String,
                           siteurl:String,
                           sitetitle:String,
                           siteh1:String,
                           sitemetaDescription:String,
                           sitemetaAuthor:String,
                           categorysector:String,
                           categoryindustryGroup:String,
                           categoryindustry:String,
                           categorysubIndustry:String,
                           description:String,
                           foundedYear:String,
                           location:String,
                           timeZone:String,
                           utcOffset:String,
                           geostreetNumber:String,
                           geostreetName:String,
                           geosubPremise:String,
                           geocity:String,
                           geopostalCode:String,
                           geostate:String,
                           geostateCode:String,
                           geocountry:String,
                           geocountryCode:String,
                           geolat:String,
                           geolng:String,
                           logo:String,
                           facebookhandle:String,
                           linkedinhandle:String,
                           twitterhandle:String,
                           twitterid:String,
                           twitterbio:String,
                           twitterfollowers:String,
                           twitterfollowing:String,
                           twitterlocation:String,
                           twittersite:String,
                           twitteravatar:String,
                           crunchbasehandle:String,
                           emailProvider:String,type1:String,ticker:String,
                           phone:String,
                           metricsalexaUsRank:String,
                           metricsalexaGlobalRank:String,
                           metricsgoogleRank:String,
                           metricsemployees:String,
                           metricsemployeesRange:String,
                           metricsmarketCap:String,
                           metricsraised:String,
                           metricsannualRevenue:String,
                           indexedAt:String)
  /*
  case class for domain alias
   */
  case class domainAliasesDetails(id:String,domainAliases:String)
  /*
  case class for site phone number
   */
  case class sitephoneNumbersDetails(id:String,sitephoneNumbers:String)
  /*
  case class for site email address
   */
  case class siteemailAddressesDetails(id:String,siteemailAddresses:String)
  /*
  case class for tagsdetails
   */
  case class tagsDetails(id:String,tags:String)

  /*
  buffer list for domain list for all fields
   */
  var DomainList:scala.collection.mutable.ListBuffer[DomainDetails]=scala.collection.mutable.ListBuffer.empty
  /*
    buffer list for domain aliase comming as list from aws api
     */
  var domainAliasesList:scala.collection.mutable.ListBuffer[domainAliasesDetails]=scala.collection.mutable.ListBuffer.empty
  /*
  buffer list for site phone  comming as list from aws api
   */
  var sitephoneNumbersList:scala.collection.mutable.ListBuffer[sitephoneNumbersDetails]=scala.collection.mutable.ListBuffer.empty
  /*
   buffer list for site email list comming as list from aws api
    */
  var siteemailAddressesList:scala.collection.mutable.ListBuffer[siteemailAddressesDetails]=scala.collection.mutable.ListBuffer.empty
  /*
  buffer list for tag list for comming as list from aws api
   */
  var tagsList:scala.collection.mutable.ListBuffer[tagsDetails]=scala.collection.mutable.ListBuffer.empty
  val appname = "ClearbitDomainVerification"
  val file_key = 1
  val started = "started"
  val ended = "ended"
  val instanceName = appname + System.currentTimeMillis()

  /*
  Spark job starts from main method
   */
  var filePath:String=null
  var myConfigFile:String=null

  def main(arg: Array[String]) = {


    val sparkConf = new SparkConf().setAppName(appname)
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().appName(appname).getOrCreate()

    filePath=arg(0)
    myConfigFile=arg(1)

    //println("filePath: " + filePath)
   // println("myConfigFile: " + myConfigFile)
   /* val sparkConf = new SparkConf().setAppName(appname).setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder.appName("ClearbitDomainVerification").config("spark.sql.warehouse.dir","file:///C:/tempwarehouse").getOrCreate()
   */

    /*
    reading all config files values
    */
    ruleMap = readEnrichmentPropFile.getval(sparkSession,filePath)
    //print("ruleMap:" + ruleMap)
    val jdbcURL=ruleMap.get("jdbcURL").get
    val dbUser=ruleMap.get("dbUser").get
    val userName=ruleMap.get("userName").get
    val dbPwd = readEnrichmentPropFile.getpassval(sparkSession,"rwpass",myConfigFile)
    val clearBitDomainUrl=ruleMap.get("clearBitDomainUrl").get
    val clearBitDomainhost=ruleMap.get("clearBitDomainhost").get
    val clearBitDomaincanonicaluri=ruleMap.get("clearBitDomaincanonicaluri").get
    val access_key=ruleMap.get("access_key").get
    val secret_key=ruleMap.get("secret_key").get
    val throttlingFactor =ruleMap.get("throttlingFactorclearBit").get
    println("throttlingFactorclearBit"+throttlingFactor)
    val timeToWaitInsecond = ruleMap.get("timeToWaitInsecondclearBit").get

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
    //print(jobStatusStart.show())
    jobStatusStart.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)
    /*
     main method for all processing for email
    */
    processRecords(sparkSession,jdbcURL,dbUser,userName,connectionProperties,clearBitDomainUrl,clearBitDomainhost,clearBitDomaincanonicaluri,access_key,secret_key,throttlingFactor,timeToWaitInsecond)
    /*
     saving spark job start and end time in  spark_jobs_run_details
    */
    val jobStatusEnd = sparkSession.sql("select distinct "+maxJobKey+" as batch_id,'"+instanceName+"'"+" as instance_name,'ended' as status,'"+file_key+"' as file_key , '"+entity_detail_key+"' as entity_detail_key ,current_timestamp() as created_date,'"+userName+"' as created_user from jobs_master_table")
    jobStatusEnd.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)
    val endTime = System.currentTimeMillis()

    sparkSession.stop()
    sparkContext.stop()
  }
   /*
   mehod will accept the spark session and process the reords sparkSession,jdbcURL,dbUser,userName,connectionProperties,clearBitDomainUrl,clearBitDomainhost,clearBitDomaincanonicaluri,access_key,secret_key,throttlingFactor,timeToWaitInsecond
   */
  def processRecords(sparkSession: SparkSession,jdbcURL:String,dbUser:String,userName:String,connectionProperties:Properties,clearBitDomainUrl:String,clearBitDomainhost:String,clearBitDomaincanonicaluri:String,access_key:String,secret_key:String,throttlingFactor:String,timeToWaitInsecond:String): Unit = {
    try {
     /*
      get all the records from account enrichment_api_person_email_verification_api_history person_email_verification_api_history table
     */
      //val dbaccount = sparkSession.read.jdbc(jdbcURL, "mdm.account", connectionProperties).select("website")//.where("website in ('engelglobal.com' ,'www.smartcopinc.com' ,'www.woolworths.com.au' ,'www.usda.gov')")

      val dbaccount = sparkSession.read.jdbc(jdbcURL, "mdm.v_batch_run_details_monthly_clearbit", connectionProperties).select("website")//.where("website in ('engelglobal.com' ,'www.smartcopinc.com' ,'www.woolworths.com.au' ,'www.usda.gov')")
      val enrichmentapipersonemailverificationapihistory = sparkSession.read.jdbc(jdbcURL, "mdm.enrichment_api_person_email_verification_api_history", connectionProperties).select("domainpart")//.where(" domainpart in ('engelglobal.com' ,'www.smartcopinc.com' ,'www.woolworths.com.au' ,'www.usda.gov') ")
      val personemailverificationapihistory = sparkSession.read.jdbc(jdbcURL, "mdm.person_email_verification_api_history", connectionProperties).select("domainpart")//.where(" domainpart in ('engelglobal.com' ,'www.smartcopinc.com' ,'www.woolworths.com.au' ,'www.usda.gov')")
      /*
      Union all above data frame
       */
      val allDomains = dbaccount.union(enrichmentapipersonemailverificationapihistory).union(personemailverificationapihistory)
      //print("\nallDomains" + allDomains.show())

      /*
      get the data from enrichment_api_company_clearbit_domain table
       */
      val enrichmentapicompanyclearbitdomain = sparkSession.read.jdbc(jdbcURL, "mdm.enrichment_api_company_clearbit_domain", connectionProperties).select("domain")
     // print("\nallback dated Domains to be filter" + enrichmentapicompanyclearbitdomain.show())
      /*
      remove the records from history table enrichment_api_company_clearbit_domain
       */
      val allDomainsFiltered = allDomains.except(enrichmentapicompanyclearbitdomain)
      //print("\nallback dated Domains Filtered" + allDomainsFiltered.show())

      //print("\nnumber of domains need to process before distinct:" + allDomainsFiltered.count())

      allDomainsFiltered.createOrReplaceTempView("allDomainsFilteredtempclearbitdomain")
      val distinctDomainDF=sparkSession.sql("select distinct website from allDomainsFilteredtempclearbitdomain")
      //print("\nall domain after removing duplicates" + distinctDomainDF.show())
      /*
      call throttling method by passing data frame
       */
      val filterallDomainsList = distinctDomainDF.toDF().collect()
      invokeAPIWithThrottling(filterallDomainsList.take(10),access_key,secret_key,clearBitDomainUrl,clearBitDomainhost,clearBitDomaincanonicaluri,throttlingFactor,timeToWaitInsecond)

    } catch {
      case e: Exception => {
        print("ERROR : Exception in processRecords ClearbitDomainVerification:"+e.toString())
        e.printStackTrace();

      }
    }
    finally {
      /*
      Saving the final result in enrichment_phone_temp and google api validated phone in phone validate google table
      */
      import sparkSession.implicits._
      if(!DomainList.isEmpty) {
       val DomainListDF = DomainList.toDF()
        //println("DomainListDF" + DomainListDF.show())
        DomainListDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.clearbit_domain_verification_temp", connectionProperties)
      }
      if(!domainAliasesList.isEmpty) {
        val domainAliasesListDF = domainAliasesList.toDF()
        //println("domainAliasesListDF" + domainAliasesListDF.show())
        domainAliasesListDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.clearbit_domain_Aliases_List_temp", connectionProperties)
      }
      if(!tagsList.isEmpty) {
        val tagsListDF = tagsList.toDF()
        //println("tagsListDF" + tagsListDF.show())
        tagsListDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.clearbit_domain_tags_List_temp", connectionProperties)
      }
      if(!sitephoneNumbersList.isEmpty) {
        val sitephoneNumbersListDF = sitephoneNumbersList.toDF()
        //println("sitephoneNumbersListDF" + sitephoneNumbersListDF.show())
        sitephoneNumbersListDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.clearbit_domain_sitephone_Numbers_List_temp", connectionProperties)
      }
      if(!siteemailAddressesList.isEmpty) {
        val siteemailAddressesListDF = siteemailAddressesList.toDF()
        //println("siteemailAddressesListDF" + siteemailAddressesListDF.show())
        siteemailAddressesListDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.clearbit_domain_site_email_Addresses_List_temp", connectionProperties)
      }
    }
  }


/*
throttling algo mehod
 */
  def invokeAPIWithThrottling(dataArray:Array[org.apache.spark.sql.Row],access_key:String,secret_key:String,clearBitDomainUrl:String,clearBitDomainhost:String,clearBitDomaincanonicaluri:String,throttlingFactor:String,timeToWaitInsecond:String)=
  {

    // val startTime = System.currentTimeMillis()
    import scala.collection.parallel._
    val dataList = dataArray.toList
    //print("\nlist"+dataList)
    if(!dataList.isEmpty)
    {
      val parallelList =dataList.par
      parallelList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(throttlingFactor.toInt))
      parallelList.map(x=>invokeAPI(x,access_key,secret_key,clearBitDomainUrl,clearBitDomainhost,clearBitDomaincanonicaluri))
    }
    //val endTime = System.currentTimeMillis()
    //println("Overall Time taken to process all records :"+(endTime-startTime )/1000)
  }


  /*
  invokeAPI method will call the aws api
   */
  def invokeAPI(row:org.apache.spark.sql.Row,access_key:String,secret_key:String,clearBitDomainUrl:String,clearBitDomainhost:String,clearBitDomaincanonicaluri:String)
  {
   clearBitDomainDetail(row.getAs("website"),com.ca.ci.enrichment.AWSRequestSigner.clearBitDomainDetailRequest(access_key,secret_key,clearBitDomainUrl,clearBitDomainhost,clearBitDomaincanonicaluri,row.getAs("website") ) )
  }
  /*
  clear bit parser method will parse the responce json and get the fields
   */
  def clearBitDomainDetail(domain: String ,response:String) = {
    //print("inside clearBitDomainDetail\n")
try {
  if (response!=null) {
    if (response.indexOf("ErrorMessage") == -1) {
      if (response.indexOf("Unexpected 'l'") == -1) {
        //print("responce clearBit Domain Detail\n"+response)
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        val resultDomain = mapper.readTree(response)
        var id: String=null
        var name: String = null
        var legalName: String = null
        var domain: String = null
        var url: String = null
        var siteurl: String = null
        var sitetitle: String = null
        var siteh1: String = null
        var sitemetaDescription: String = null
        var sitemetaAuthor: String = null
        var categorysector: String = null
        var categoryindustryGroup: String = null
        var categoryindustry: String = null
        var categorysubIndustry: String = null
        var description: String = null
        var foundedYear: String = null
        var location: String = null
        var timeZone: String = null
        var utcOffset: String = null
        var geostreetNumber: String = null
        var geostreetName: String = null
        var geosubPremise: String = null
        var geocity: String = null
        var geopostalCode: String = null
        var geostate: String = null
        var geostateCode: String = null
        var geocountry: String = null
        var geocountryCode: String = null
        var geolat: String = null
        var geolng: String = null
        var vargeolng: String = null
        var logo: String = null
        var facebookhandle: String = null
        var linkedinhandle: String = null
        var twitterhandle: String = null
        var twitterid: String = null
        var twitterbio: String = null
        var twitterfollowers: String = null
        var twitterfollowing: String = null
        var twitterlocation: String = null
        var twittersite: String = null
        var twitteravatar: String = null
        var crunchbasehandle: String = null
        var emailProvider: String = null
        var type1: String = null
        var ticker: String = null
        var phone: String = null
        var metricsalexaUsRank: String = null
        var metricsalexaGlobalRank: String = null
        var metricsgoogleRank: String = null
        var metricsemployees: String = null
        var metricsemployeesRange: String = null
        var metricsmarketCap: String = null
        var metricsraised: String = null
        var metricsannualRevenue: String = null
        var indexedAt: String = null

        if (resultDomain.has("id")) {
          if(!resultDomain.get("id").asText().equalsIgnoreCase("null")) {
            id = resultDomain.get("id").asText()
          }
        }
        if (resultDomain.has("name")) {
          if(!resultDomain.get("name").asText().equalsIgnoreCase("null")) {
            name = resultDomain.get("name").asText()
          }
        }
        if (resultDomain.has("legalName")) {
          if(!resultDomain.get("legalName").asText().equalsIgnoreCase("null")) {
            legalName = resultDomain.get("legalName").asText()
          }
        }
        if (resultDomain.has("domain")) {
          if(!resultDomain.get("domain").asText().equalsIgnoreCase("null")) {
            domain = resultDomain.get("domain").asText()
          }
        }
        if (resultDomain.has("url")) {
          if(!resultDomain.get("url").asText().equalsIgnoreCase("null")) {
            url = resultDomain.get("url").asText()
          }
        }

        if (resultDomain.has("site")) {
          if (resultDomain.get("site").has("url")) {
            if(!resultDomain.get("site").get("url").asText().equalsIgnoreCase("null")) {
              siteurl = resultDomain.get("site").get("url").asText()
            }
          }
          if (resultDomain.get("site").has("title")) {
            if(!resultDomain.get("site").get("title").asText().equalsIgnoreCase("null")) {
              sitetitle = resultDomain.get("site").get("title").asText()
            }
          }
          if (resultDomain.get("site").has("h1")) {
            if(!resultDomain.get("site").get("h1").asText().equalsIgnoreCase("null")) {
              siteh1 = resultDomain.get("site").get("h1").asText()
            }
          }
          if (resultDomain.get("site").has("metaDescription")) {
            if(!resultDomain.get("site").get("metaDescription").asText().equalsIgnoreCase("null")) {
              sitemetaDescription = resultDomain.get("site").get("metaDescription").asText()
            }
          }
          if (resultDomain.get("site").has("metaAuthor")) {
            if(!resultDomain.get("site").get("metaAuthor").asText().equalsIgnoreCase("null")) {
            sitemetaAuthor = resultDomain.get("site").get("metaAuthor").asText()
          }
          }
        }

        if (resultDomain.has("category")) {
          if (resultDomain.get("category").has("sector")) {
            if (!resultDomain.get("category").get("sector").asText().equalsIgnoreCase("null")) {
              categorysector = resultDomain.get("category").get("sector").asText()
            }
            }
            if (resultDomain.get("category").has("industryGroup")) {
              if (!resultDomain.get("category").get("industryGroup").asText().equalsIgnoreCase("null")) {
                categoryindustryGroup = resultDomain.get("category").get("industryGroup").asText()
              }
            }
            if (resultDomain.get("category").has("industry")) {
              if(!resultDomain.get("category").get("industry").asText().equalsIgnoreCase("null")) {
                categoryindustry = resultDomain.get("category").get("industry").asText()
              }
            }
            if (resultDomain.get("category").has("subIndustry")) {
              if(!resultDomain.get("category").get("subIndustry").asText().equalsIgnoreCase("null")) {
                categorysubIndustry = resultDomain.get("category").get("subIndustry").asText()
              }
            }
          }
        if (resultDomain.has("description")) {
          if(!resultDomain.get("description").asText().equalsIgnoreCase("null")) {
            description = resultDomain.get("description").asText()
          }
        }
        if (resultDomain.has("foundedYear")) {
          if(!resultDomain.get("foundedYear").asText().equalsIgnoreCase("null")) {
            foundedYear = resultDomain.get("foundedYear").asText()
          }
        }
        if (resultDomain.has("location")) {
          if(!resultDomain.get("location").asText().equalsIgnoreCase("null")) {
            location = resultDomain.get("location").asText()
          }
        }
        if (resultDomain.has("timeZone")) {
          if(!resultDomain.get("timeZone").asText().equalsIgnoreCase("null")) {
            timeZone = resultDomain.get("timeZone").asText()
          }
        }
        if (resultDomain.has("utcOffset")) {
          if(!resultDomain.get("utcOffset").asText().equalsIgnoreCase("null")) {
            utcOffset = resultDomain.get("utcOffset").asText()
          }
        }

        if (resultDomain.has("geo")) {
          if (resultDomain.get("geo").has("streetNumber")) {
            if(!resultDomain.get("geo").get("streetNumber").asText().equalsIgnoreCase("null")) {
              geostreetNumber = resultDomain.get("geo").get("streetNumber").asText()
            }
          }
          if (resultDomain.get("geo").has("streetName")) {
            if(!resultDomain.get("geo").get("streetName").asText().equalsIgnoreCase("null"))
            {
              geostreetName = resultDomain.get("geo").get("streetName").asText()
            }
          }
          if (resultDomain.get("geo").has("subPremise")) {
            if(!resultDomain.get("geo").get("subPremise").asText().equalsIgnoreCase("null"))
            {
              geosubPremise = resultDomain.get("geo").get("subPremise").asText()
            }
          }
          if (resultDomain.get("geo").has("city")) {
            if(!resultDomain.get("geo").get("city").asText().equalsIgnoreCase("null"))
            {
              geocity = resultDomain.get("geo").get("city").asText()
            }
          }
          if (resultDomain.get("geo").has("postalCode")) {
            if(!resultDomain.get("geo").get("postalCode").asText().equalsIgnoreCase("null"))
            {
              geopostalCode = resultDomain.get("geo").get("postalCode").asText()
            }
          }
          if (resultDomain.get("geo").has("state")) {
            if(!resultDomain.get("geo").get("state").asText().equalsIgnoreCase("null"))
            {
              geostate = resultDomain.get("geo").get("state").asText()
            }
          }
          if (resultDomain.get("geo").has("stateCode")) {
            if(!resultDomain.get("geo").get("stateCode").asText().equalsIgnoreCase("null"))
            {
              geostateCode = resultDomain.get("geo").get("stateCode").asText()
            }
          }
          if (resultDomain.get("geo").has("country")) {
            if(!resultDomain.get("geo").get("country").asText().equalsIgnoreCase("null"))
            {
              geocountry = resultDomain.get("geo").get("country").asText()
            }
          }
          if (resultDomain.get("geo").has("countryCode")) {
            if(!resultDomain.get("geo").get("countryCode").asText().equalsIgnoreCase("null"))
            {
              geocountryCode = resultDomain.get("geo").get("countryCode").asText()
            }
          }
          if (resultDomain.get("geo").has("lat")) {
            if(!resultDomain.get("geo").get("lat").asText().equalsIgnoreCase("null"))
            {
              geolat = resultDomain.get("geo").get("lat").asText()
            }
          }
          if (resultDomain.get("geo").has("lng")) {
            if(!resultDomain.get("geo").get("lng").asText().equalsIgnoreCase("null"))
            {
              geolng = resultDomain.get("geo").get("lng").asText()
            }
          }
        }

        if (resultDomain.has("logo")) {
          if(!resultDomain.get("logo").asText().equalsIgnoreCase("null"))
          {
            logo = resultDomain.get("logo").asText()
          }
        }
        if (resultDomain.has("facebook")) {

          if (resultDomain.get("facebook").has("handle")) {
            if(!resultDomain.get("facebook").get("handle").asText().equalsIgnoreCase("null"))
            {
              facebookhandle = resultDomain.get("facebook").get("handle").asText()
            }
          }
        }
        if (resultDomain.has("linkedin")) {
          if (resultDomain.get("linkedin").has("handle")) {
            if(!resultDomain.get("linkedin").get("handle").asText().equalsIgnoreCase("null"))
            {
              linkedinhandle = resultDomain.get("linkedin").get("handle").asText()
            }
          }
        }
        if (resultDomain.has("twitter")) {

          if (resultDomain.get("twitter").has("handle")) {
            if(!resultDomain.get("twitter").get("handle").asText().equalsIgnoreCase("null"))
            {
              twitterhandle = resultDomain.get("twitter").get("handle").asText()
            }
          }
          if (resultDomain.get("twitter").has("id")) {
            if(!resultDomain.get("twitter").get("id").asText().equalsIgnoreCase("null"))
            {
              twitterid = resultDomain.get("twitter").get("id").asText()
            }
          }
          if (resultDomain.get("twitter").has("bio")) {
            if( !resultDomain.get("twitter").get("bio").asText().equalsIgnoreCase("null"))
            {
              twitterbio = resultDomain.get("twitter").get("bio").asText()
            }
          }
          if (resultDomain.get("twitter").has("followers")) {
            if(!resultDomain.get("twitter").get("followers").asText().equalsIgnoreCase("null"))
            {
              twitterfollowers = resultDomain.get("twitter").get("followers").asText()
            }
          }
          if (resultDomain.get("twitter").has("following")) {
            if(!resultDomain.get("twitter").get("following").asText().equalsIgnoreCase("null"))
            {
              twitterfollowing = resultDomain.get("twitter").get("following").asText()
            }
          }
          if (resultDomain.get("twitter").has("location")) {
            if(!resultDomain.get("twitter").get("location").asText().equalsIgnoreCase("null"))
            {
              twitterlocation = resultDomain.get("twitter").get("location").asText()
            }
          }
          if (resultDomain.get("twitter").has("site")) {
            if(!resultDomain.get("twitter").get("site").asText().equalsIgnoreCase("null"))
            {
              twittersite = resultDomain.get("twitter").get("site").asText()
            }
          }
          if (resultDomain.get("twitter").has("avatar")) {
            if(!resultDomain.get("twitter").get("avatar").asText().equalsIgnoreCase("null"))
            {
              twitteravatar = resultDomain.get("twitter").get("avatar").asText()
            }
          }
        }

        if (resultDomain.has("crunchbase")) {
          if (resultDomain.get("crunchbase").has("handle")) {
            if(!resultDomain.get("crunchbase").get("handle").asText().equalsIgnoreCase("null"))
            {
              crunchbasehandle = resultDomain.get("crunchbase").get("handle").asText()
            }
          }
        }
        if (resultDomain.has("emailProvider")) {
          if(!resultDomain.get("emailProvider").asText().equalsIgnoreCase("null"))
          {
            emailProvider = resultDomain.get("emailProvider").asText()
          }
        }
        if (resultDomain.has("type")) {
          if(!resultDomain.get("type").asText().equalsIgnoreCase("null"))
          {
            type1 = resultDomain.get("type").asText()
          }
        }
        if (resultDomain.has("ticker")) {
          if(!resultDomain.get("ticker").asText().equalsIgnoreCase("null"))
          {
            ticker = resultDomain.get("ticker").asText()
          }
        }
        if (resultDomain.has("phone")) {
          if(!resultDomain.get("phone").asText().equalsIgnoreCase("null"))
          {
            phone = resultDomain.get("phone").asText()
          }
        }
        if (resultDomain.has("metrics")) {
          if (resultDomain.get("metrics").has("alexaUsRank")) {
            if(!resultDomain.get("metrics").get("alexaUsRank").asText().equalsIgnoreCase("null"))
            {
              metricsalexaUsRank = resultDomain.get("metrics").get("alexaUsRank").asText()
            }
          }
          if (resultDomain.get("metrics").has("alexaGlobalRank")) {
            if(!resultDomain.get("metrics").get("alexaGlobalRank").asText().equalsIgnoreCase("null"))
            {
              metricsalexaGlobalRank = resultDomain.get("metrics").get("alexaGlobalRank").asText()
            }
          }
          if (resultDomain.get("metrics").has("googleRank")) {
            if(!resultDomain.get("metrics").get("googleRank").asText().equalsIgnoreCase("null"))
            {
              metricsgoogleRank = resultDomain.get("metrics").get("googleRank").asText()
            }
          }
          if (resultDomain.get("metrics").has("employees")) {
            if(!resultDomain.get("metrics").get("employees").asText().equalsIgnoreCase("null"))
            {
              metricsemployees = resultDomain.get("metrics").get("employees").asText()
            }
          }
          if (resultDomain.get("metrics").has("employeesRange")) {
            if(!resultDomain.get("metrics").get("employeesRange").asText().equalsIgnoreCase("null"))
            {
              metricsemployeesRange = resultDomain.get("metrics").get("employeesRange").asText()
            }
          }
          if (resultDomain.get("metrics").has("marketCap")) {
            if(!resultDomain.get("metrics").get("marketCap").asText().equalsIgnoreCase("null"))
            {
              metricsmarketCap = resultDomain.get("metrics").get("marketCap").asText()
            }
          }
          if (resultDomain.get("metrics").has("raised")) {
            if(!resultDomain.get("metrics").get("raised").asText().equalsIgnoreCase("null"))
            {
              metricsraised = resultDomain.get("metrics").get("raised").asText()
            }
          }
          if (resultDomain.get("metrics").has("annualRevenue")) {
            if(!resultDomain.get("metrics").get("annualRevenue").asText().equalsIgnoreCase("null"))
            {
              metricsannualRevenue = resultDomain.get("metrics").get("annualRevenue").asText()
            }
          }
        }

        if (!id.equals("")) {
          var domainAliases: String = null
          if (resultDomain.has("domainAliases")) {
            val itrdomainAliases = resultDomain.get("domainAliases").iterator()
            var count = 0
            while (itrdomainAliases.hasNext) {
              domainAliases=null
              if (resultDomain.get("domainAliases").has(count)) {
                domainAliases = resultDomain.get("domainAliases").get(count).asText()
              }
              domainAliasesList.append(domainAliasesDetails(id, domainAliases))
              itrdomainAliases.next()
              count = count + 1
            }
          }
        }

        if (!id.equals("")) {
          var tags: String = null
          if (resultDomain.has("tags")) {
            val itrtags = resultDomain.get("tags").iterator()
            var counttags = 0
            while (itrtags.hasNext) {
              tags=null
              if (resultDomain.get("tags").has(counttags)) {
                tags = resultDomain.get("tags").get(counttags).asText()
                //print("\ntags 1*\n" + tags)
              }
              tagsList.append(tagsDetails(id, tags))
              itrtags.next()
              counttags = counttags + 1
            }
          }
        }


        if (!id.equals("")) {
          if (resultDomain.has("site")) {
            if (resultDomain.get("site").has("phoneNumbers")) {
              var sitephoneNumbers: String = null
              val itrphone = resultDomain.get("site").get("phoneNumbers").iterator()
              var countphone = 0
              while (itrphone.hasNext) {
                sitephoneNumbers=null
                if (resultDomain.get("site").get("phoneNumbers").has(countphone)) {
                  sitephoneNumbers = resultDomain.get("site").get("phoneNumbers").get(countphone).asText()
                }
                sitephoneNumbersList.append(sitephoneNumbersDetails(id, sitephoneNumbers))
                itrphone.next()
                countphone = countphone + 1
              }
            }
          }
        }

        if (!id.equals("")) {
          if (resultDomain.has("site")) {
            if (resultDomain.get("site").has("emailAddresses")) {
              var siteemailAddresses: String = null
              val itremail = resultDomain.get("site").get("emailAddresses").iterator()
              var countemail = 0
              while (itremail.hasNext) {
                siteemailAddresses=null
                if (resultDomain.get("site").get("emailAddresses").has(countemail)) {
                  siteemailAddresses = resultDomain.get("site").get("emailAddresses").get(countemail).asText()
                }
                siteemailAddressesList.append(siteemailAddressesDetails(id, siteemailAddresses))
                itremail.next()
                countemail = countemail + 1
              }
            }
          }
        }

        if (resultDomain.has("indexedAt")) {
          indexedAt = resultDomain.get("indexedAt").asText()
        }

        if (!id.equals("")) {
          DomainList.append(DomainDetails(id,
            name,
            legalName,
            domain,
            url,
            siteurl,
            sitetitle,
            siteh1,
            sitemetaDescription,
            sitemetaAuthor,
            categorysector,
            categoryindustryGroup,
            categoryindustry,
            categorysubIndustry,
            description,
            foundedYear,
            location,
            timeZone,
            utcOffset,
            geostreetNumber,
            geostreetName,
            geosubPremise,
            geocity,
            geopostalCode,
            geostate,
            geostateCode,
            geocountry,
            geocountryCode,
            geolat,
            geolng,
            logo,
            facebookhandle,
            linkedinhandle,
            twitterhandle,
            twitterid,
            twitterbio,
            twitterfollowers,
            twitterfollowing,
            twitterlocation,
            twittersite,
            twitteravatar,
            crunchbasehandle,
            emailProvider,
            type1,
            ticker,
            phone,
            metricsalexaUsRank,
            metricsalexaGlobalRank,
            metricsgoogleRank,
            metricsemployees,
            metricsemployeesRange,
            metricsmarketCap,
            metricsraised,
            metricsannualRevenue,
            indexedAt))
        }
      }else
      {
        print("\nERROR: For domain:"+domain+"\nresponse"+response)
      }
    }else
      {
        print("\nERROR: For domain:"+domain+"\nresponse"+response)
      }
  }
} catch {
    case e: Exception => {
      print("\nERROR:Exception in clearBitDomainDetail in response got from aws call response is :"+response+"\ndomain:"+domain+ e.toString())
      e.printStackTrace();

    }
  }
  }
}
