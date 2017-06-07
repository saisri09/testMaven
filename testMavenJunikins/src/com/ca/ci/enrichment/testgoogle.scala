package com.ca.ci.enrichment

import com.ca.ci.enrichment.PhoneValidationGoogle.PhoneDetails
import com.google.i18n.phonenumbers.PhoneNumberUtil
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber

/**
  * Created by malsa15 on 6/7/2017.
  */
object testgoogle {

  //var StandardizationPhoneList:scala.collection.mutable.ListBuffer[PhoneDetails]=scala.collection.mutable.ListBuffer.empty

  case class PhoneDetails(person_phone_key:String,standardised_phone_number:String,isValid:String,country_code:String,telephone_number:String)
  //case class PhoneDetails(telephone_number:String,standardised_phone_number:String,is_valid_flag:String,person_phone_key:String,country:String)
  /*
  this method will format the phone number based on US format and send back to called method
   */

  def main(args: Array[String]): Unit = {
    var output: PhoneDetails = null
    try {
      val phoneUtil: PhoneNumberUtil = PhoneNumberUtil.getInstance()
      /*
      parse the phone in US phone format
     */
      var NumberProto: PhoneNumber=null

        NumberProto = phoneUtil.parse("9493056091", "IN")
      /*
      if its valid phone then send back the result as method will return null
      */
      val isValid: Boolean = phoneUtil.isValidNumber(NumberProto)
      println(isValid)
      println(phoneUtil.format(NumberProto, PhoneNumberFormat.E164))
      //println("phone:" + telephone_number + ":" + isValid);
      //if (isValid) {
      //println(NumberProto.getNationalNumber)
      //  output = PhoneDetails(person_phone_key, NumberProto.getNationalNumber.toString, isValid.toString, country_code, telephone_number)
      output = PhoneDetails("1", phoneUtil.format(NumberProto, PhoneNumberFormat.E164), isValid.toString, "IN", "9493056091")
      // }
      //return output
    }catch
      {
        case e: Exception => {
        //  print("\nERROR :Exception in googlePhoneValidation method for telephone_number:"+telephone_number+"\nCountry Code:"+country_code+"\n"+e.toString)
          e.printStackTrace()

        }
      }
  }
}
