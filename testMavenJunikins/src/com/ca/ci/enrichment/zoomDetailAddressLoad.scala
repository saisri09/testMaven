package com.ca.ci.enrichment

import java.io.IOException
import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by nagma05 on 1/20/2017.
  */



/***
  *
This spark job will read the data from the blob storage file , normalize the data fields like mailing address and others address and loads into mdm.address table.
  *
  */


object zoomDetailAddressLoad {
  var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty

  def loadMailingAddress(sparkSession: SparkSession, connectionProperties: Properties, jdbcURL:String,userName:String): Unit = {

    val dbCurrentEmp = sparkSession.read.jdbc(jdbcURL, "mdm.Person_Detail_Current_Employment_temp", connectionProperties)//.select("source_system_person_id","person_key").where("source_system_key=1 ")
    dbCurrentEmp.createOrReplaceTempView("dbCurrentEmptempview")

    val resDF_temp_m=sparkSession.sql("select CurrentCompanyID ,substring(CurrentAddressStreet,1,100) as street1_name,substring(CurrentAddressStreet,101,200) as street2_name,substring(CurrentAddressStreet,201,300) as street3_name,substring(CurrentAddressStreet,301,400) as street4_name,substring(CurrentAddressStreet,401) as street5_name,CurrentAddressCity as city,CurrentAddressState as state,CurrentAddressZip as  postal_code,CurrentAddressCountryCode as country from dbCurrentEmptempview ")
    resDF_temp_m.createOrReplaceTempView("resDF_temp_table_m")
    //print(resDF_temp_m.show(5))


    val dbAddress = sparkSession.read.jdbc(jdbcURL, "mdm.address", connectionProperties).select("hash_key_val")//.where("source_system_key=1 ")
    dbAddress.createOrReplaceTempView("dbaddress")
    //print("in table loading")


    val dbCountry = sparkSession.read.jdbc(jdbcURL, "mdm.country_address_state_vw", connectionProperties)
    dbCountry.createOrReplaceTempView("db_country_state_view")
    //print("in table loading dbCountry" +dbCountry)


   val bobHash_m=sparkSession.sql("select a.*,hash(a.street1_name,a.street2_name,a.street3_name,a.street4_name,a.street5_name,a.city,a.country,a.state,a.postal_code ) as hash_key_val from resDF_temp_table_m a")
    //print(bobHash_m.show(6))

    bobHash_m.createOrReplaceTempView("temptable")
    val blobAddress=sparkSession.sql("select a.hash_key_val from temptable a")

    val blobAddresstemp=blobAddress.except(dbAddress)
    //print("blobAddresstemp"+blobAddresstemp.show(6))
    blobAddresstemp.createOrReplaceTempView("blobAddresstemp_tab")

    val dbAddressTemp=sparkSession.sql("select a.* from temptable a ,blobAddresstemp_tab b   where a.hash_key_val = b.hash_key_val")
     dbAddressTemp.createOrReplaceTempView("dbAddressTempTable")
    //print("dbAddressTemp"+dbAddressTemp.show(6))


    val blobAddressCityTemp=sparkSession.sql("""select a.*,b.address_state_key from dbAddressTempTable a left outer join db_country_state_view b
              on ( upper(trim(a.country)) = upper(trim(b.country_synonym)) and upper(trim(a.state))=upper(trim(b.state_name)))""")
    blobAddressCityTemp.createOrReplaceTempView("blob_address_state_key_temp")
    //print("blobAddressCityTemp"+blobAddressCityTemp.show(6))
    val result_temp=sparkSession.sql(
      """select distinct CurrentCompanyID,substring(a.street1_name,1,100) as street1_name
         ,substring(a.street2_name,1,100) as street2_name
         ,substring(a.street3_name,1,100) as street3_name
         ,substring(a.street4_name,1,100) as street4_name
         ,substring(a.street5_name,1,100) as street5_name
         ,substring(a.city,1,100) as city
         ,substring(a.country,1,255) as country
         ,substring(a.state,1,255) as state
         ,substr(a.postal_code,1,50) as postal_code
         ,a.address_state_key
         ,hash_key_val
         ,0 as update_flag
         from blob_address_state_key_temp a """)

   // println("result_temp"+result_temp.show())
    result_temp.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.company_address_temp", connectionProperties)



  }


  var filePath:String=null
  var myConfigFile:String=null
   def main(arg: Array[String]) {
    try {

      val jobName="sfdc_account_load"


      val sparkconf = new SparkConf().setAppName(jobName)
      val sc = new SparkContext(sparkconf)
      val sparkSession = SparkSession.builder().appName(jobName).getOrCreate()
      filePath=arg(0)
      myConfigFile=arg(1)
      ruleMap = readEnrichmentPropFile.getval(sparkSession,filePath)

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
      val instanceName="Zoom_Info_Enrichment_AddressLoad_"+System.currentTimeMillis()

      val entitydetail = sparkSession.read.jdbc(jdbcURL, "mdm.entity_detail", connectionProperties).select("entity_detail_key").where(" entity_name='ZoomInfo_Person_Match_Temp' ")

      val entity_detail_key=entitydetail.collect().toList(0).get(0).toString
      //print("entity_detail_key: "+entity_detail_key)

      val jobStatusStart = sparkSession.sql("""select distinct """+maxJobKey+""" as batch_id,'"""+instanceName+"""' as instance_name,'started' as status,'"""+file_key+"""' as file_key ,
      '"""+entity_detail_key+"""' as entity_detail_key ,current_timestamp() as created_date,'"""+userName+"""' as created_user
      from jobs_master_table a """)
      //print(jobStatusStart.show())
      jobStatusStart.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)


      loadMailingAddress(sparkSession, connectionProperties,jdbcURL,userName)

      /*
      saving spark job start and end time in  spark_jobs_run_details
          */
      val jobStatusEnd = sparkSession.sql("select distinct "+maxJobKey+" as batch_id,'"+instanceName+"'"+" as instance_name,'ended' as status,'"+file_key+"' as file_key , '"+entity_detail_key+"' as entity_detail_key ,current_timestamp() as created_date,'"+userName+"' as created_user from jobs_master_table")
      jobStatusEnd.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)
      val endTime = System.currentTimeMillis()

      sparkSession.stop()
      sc.stop()
    }
    catch {
      case e: IOException => { e.printStackTrace(); e.toString() }
    }

  }
}