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
  *
This spark job will read the data from the blob storage file , normalize the data fields like mailing address and others address and provides the mapping between person & address.
  *
  */


object zoomDetailBridgeAddressLoad {
  var ruleMap:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty

  def loadbridgePersonMailingAddress(sparkSession: SparkSession, connectionProperties: Properties, jdbcURL:String,userName:String): Unit = {

    val dbcompanyaddresstemp = sparkSession.read.jdbc(jdbcURL, "mdm.company_address_temp", connectionProperties)//.select("source_system_person_id","person_key").where("source_system_key=1 ")
    dbcompanyaddresstemp.createOrReplaceTempView("company_address_temp")

    val resDF_temp_m=sparkSession.sql("select CurrentCompanyID ,street1_name,street2_name,street3_name,street4_name,street5_name,city,state,postal_code,country from company_address_temp ")
    resDF_temp_m.createOrReplaceTempView("resDF_temp_table_m_bridge")
   // print(resDF_temp_m.show(5))

    val dbAddress = sparkSession.read.jdbc(jdbcURL, "mdm.address", connectionProperties).select("address_key","hash_key_val")//.where("source_system_key=1 ")
    dbAddress.createOrReplaceTempView("dbaddress")


    val dbenrichmentapicompany = sparkSession.read.jdbc(jdbcURL, "mdm.enrichment_api_company", connectionProperties).select("enrichment_api_company_id","enrichment_api_company_key")
    dbenrichmentapicompany.createOrReplaceTempView("enrichment_api_company_temp_view")
   // print("in table loading")

    //join dbperson and dbCurrentEmp

    val dbbridgeEnrichment = sparkSession.read.jdbc(jdbcURL, "mdm.bridge_enrichment_api_company_address", connectionProperties).select("address_type_key","enrichment_api_company_key","address_key")
    dbbridgeEnrichment.createOrReplaceTempView("bridge_enrichment_api_company_address_temp_view")
    //print("in table loading")


    val bobHash_m=sparkSession.sql("select a.*,30000031 as address_type_key,hash(a.street1_name,a.street2_name,a.street3_name,a.street4_name,a.street5_name,a.city,a.country,a.state,a.postal_code ) as hash_key_val from resDF_temp_table_m_Bridge a")
    bobHash_m.createOrReplaceTempView("temptableBridge")
    //print(bobHash_m.show(4))

    val dbAddressPerson=sparkSession.sql("select b.enrichment_api_company_key,a.address_type_key,a.CurrentCompanyID,a.hash_key_val from temptableBridge a ,enrichment_api_company_temp_view b where a.CurrentCompanyID = b.enrichment_api_company_id")
    dbAddressPerson.createOrReplaceTempView("db_address_person")
    //print(dbAddressPerson.show(4))
    val dbAddressPersonTemp=sparkSession.sql("select a.enrichment_api_company_key,b.address_key,a.address_type_key,a.hash_key_val from db_address_person a ,dbaddress b   where a.hash_key_val = b.hash_key_val")
    dbAddressPersonTemp.createOrReplaceTempView("db_address_person_temp")
    //print(dbAddressPersonTemp.show(3))

    val resultTemp=sparkSession.sql(
      """select distinct a.enrichment_api_company_key,a.address_type_key,a.address_key,if(b.enrichment_api_company_key is null,0,1) as update_flag
        from db_address_person_temp a left outer join bridge_enrichment_api_company_address_temp_view b on (a.enrichment_api_company_key = b.enrichment_api_company_key and a.address_type_key=b.address_type_key and a.address_key <> b.address_key) """)
    resultTemp.createOrReplaceTempView("result_temp")
    //resultTemp.createOrReplaceTempView("result_temp")

    print("final"+resultTemp.show(4))
    resultTemp.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.company_bridge_address_temp", connectionProperties)

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
      val instanceName="Zoom_Info_Enrichment_BridgeAddressLoad_"+System.currentTimeMillis()

      val entitydetail = sparkSession.read.jdbc(jdbcURL, "mdm.entity_detail", connectionProperties).select("entity_detail_key").where(" entity_name='ZoomInfo_Person_Match_Temp' ")

      val entity_detail_key=entitydetail.collect().toList(0).get(0).toString
      //print("entity_detail_key: "+entity_detail_key)

      val jobStatusStart = sparkSession.sql("""select distinct """+maxJobKey+""" as batch_id,'"""+instanceName+"""' as instance_name,'started' as status,'"""+file_key+"""' as file_key ,
      '"""+entity_detail_key+"""' as entity_detail_key ,current_timestamp() as created_date,'"""+userName+"""' as created_user
      from jobs_master_table a """)
      //print(jobStatusStart.show())
      jobStatusStart.write.mode(SaveMode.Append).jdbc(jdbcURL, "mdm.spark_jobs_run_details", connectionProperties)

      loadbridgePersonMailingAddress(sparkSession, connectionProperties,jdbcURL,userName)

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
