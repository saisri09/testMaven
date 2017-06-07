package com.ca.ci.enrichment

import com.google.i18n.phonenumbers.PhoneNumberUtil
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber

/**
  * Created by punma03 on 3/7/2017.
  */
object PhoneValidationGoogle {
  /*
    Google api to validate the phone
     */
  var StandardizationPhoneList:scala.collection.mutable.ListBuffer[PhoneDetails]=scala.collection.mutable.ListBuffer.empty

  case class PhoneDetails(person_phone_key:String,standardised_phone_number:String,isValid:String,country_code:String,telephone_number:String)
  //case class PhoneDetails(telephone_number:String,standardised_phone_number:String,is_valid_flag:String,person_phone_key:String,country:String)
  /*
  this method will format the phone number based on US format and send back to called method
   */

  //telephone_number,person_phone_key,country
   def googlePhoneValidation(telephone_number:String,person_phone_key:String,country_code:String):PhoneDetails = {
     //println("*****inside googlePhoneValidation *****")
     var output: PhoneDetails = null
     try {
       val phoneUtil: PhoneNumberUtil = PhoneNumberUtil.getInstance()
       /*
       parse the phone in US phone format
      */
       var NumberProto: PhoneNumber=null

       //println("*****inside country_code *****"+country_code)
       //println("*****inside telephone_number *****"+telephone_number)
       if(country_code!=null) {
         //println("*****country_code *****"+country_code)
         NumberProto = phoneUtil.parse(telephone_number, country_code)
       }else
       {
         //println("*****inside elsecountry_code *****"+country_code)
         NumberProto = phoneUtil.parse(telephone_number, "US")
       }
       /*
       if its valid phone then send back the result as method will return null
       */
       val isValid: Boolean = phoneUtil.isValidNumber(NumberProto)
       //println("phone:" + telephone_number + ":" + isValid);
       //if (isValid) {
       //println(NumberProto.getNationalNumber)
     //  output = PhoneDetails(person_phone_key, NumberProto.getNationalNumber.toString, isValid.toString, country_code, telephone_number)
       output = PhoneDetails(person_phone_key, NumberProto.getNationalNumber.toString, isValid.toString, country_code, telephone_number)
       // }
       //return output
     }catch
       {
         case e: Exception => {
           print("\nERROR :Exception in googlePhoneValidation method for telephone_number:"+telephone_number+"\nCountry Code:"+country_code+"\n"+e.toString)
           e.printStackTrace()

         }
       }
     return output
   }
}
