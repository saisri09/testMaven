package com.ca.ci.enrichment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SimpleTimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.escape.Escaper;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.net.URLCodec;
import scala.reflect.internal.Trees;

import com.google.common.net.*;

/*
Class to invoke the aws api with api keys and signature
 */
public class AWSRequestSigner {
	/*public static int countstrikeiron=0;
	public static int countmelissa=0;
	public static int countPersonMatch=0;
	public static int countPersonDetail=0;
	public static int countclearbit=0;*/

	/*
	method call by clear bit saprk job domain name
	 */
	public static String clearbitDomainVerification(String domain) {
		System.out.print("clearbitDomainVerification:");
		String urlPerson="https://d8qnprayak.execute-api.us-east-1.amazonaws.com/DEV/IMSS_DOMAIN_ENRICHMENT";
		String host="d8qnprayak.execute-api.us-east-1.amazonaws.com";
		String canonical_uri = "/DEV/IMSS_DOMAIN_ENRICHMENT";
		String query="domain="+domain+"&outputFormat=json";
		System.out.print("query:"+query);
		//String outPut=awsApiHttpCall(query,urlPerson,host,canonical_uri);
		//return outPut;
		return "";
	}


	public static String melissaPhoneVerificationReverse(String access_key,String  secret_key,String MelissaReverseUrl,String MelissaReversehost,String MelissaReversecanonicaluri,String telephone_number) {
		//System.out.print("melissaPhoneVerificationReverse:");
		String queryString="Action=Check"+"&PhoneNumber="+telephone_number+"&outputFormat=json";
		String response=awsApiHttpCall(access_key,secret_key,queryString,MelissaReverseUrl,MelissaReversehost,MelissaReversecanonicaluri);
		return response;
	}

	/*
	called by zoom info spark job to get person match records by passing first name last name email
	 */
	public static String zoomApiPersonMatch(String access_key,String secret_key,String zoominfoPersonMatchUrl,String zoominfoPersonMatchHost,String zoominfoPersonMatchcanonicaluri,String first_name,String last_name,String email_address) {



		URLCodec codec = new URLCodec();
		Escaper esc =
				new PercentEscaper("-._~", false); //!$'()*,;@:/?

		String emailaddress=null;
		String firstname= null;
		String lastname=null;
		try {
			emailaddress=esc.escape(email_address);
			firstname=esc.escape(first_name);
			lastname=esc.escape(last_name);

		}
		catch(Exception ex)
		{
			ex.printStackTrace();

		}
		//String emailaddress=email_address.replace("@","%40").replace("'","%27").replace("?","%3F").replace("/","%2F").replace("+","%2B").replace("!","%21").replace("#","%23").replace("$","%24").replace("&","%26").replace("*","%2A");


		String query = "emailAddress=" + emailaddress + "&firstname=" + firstname + "&lastName=" + lastname + "&outputFormat=json";
		//System.out.print("\n query in method zoomApiPersonMatch:" + query);
		String outPut = awsApiHttpCall(access_key,secret_key,query,zoominfoPersonMatchUrl,zoominfoPersonMatchHost,zoominfoPersonMatchcanonicaluri);
		return outPut;
	}

     /*
      call by zoom info spark job to get the person detail based on personid
     */
	public static String zoomApiPersonDetail(String access_key,String secret_key,String zoominfoPersonDetailUrl,String zoominfoPersonDetailHost,String zoominfoPersonDetailcanonicaluri,String personId) {
		//System.out.print("aws request zoomApiPersonDetail:");
		String query="PersonID="+personId+"&outputFieldOptions=jobFunction%2CmanagementLevel%2CcompanyTopLevelIndustry"+"&outputFormat=json";;
		String outPut=awsApiHttpCall(access_key,secret_key,query,zoominfoPersonDetailUrl,zoominfoPersonDetailHost,zoominfoPersonDetailcanonicaluri);
		return outPut;
	}

	/*
	method will call by melissa spark job for phone verification will pass phonenumber
	 */
	public static String melissaPhoneVerification(String accesskey,String secretkey,String melissaurl,String melissahost,String melissacanonicaluri,String phoneNumber,String country_code) {
		//System.out.print("melissaPhoneVerification:"+phoneNumber);
		//System.out.print("melissaPhoneVerification:"+country_code);
		String query=null;
		if(country_code!=null)
		{
		query = "Country="+country_code+"&PhoneNumber="+phoneNumber+"&outputFormat=json";
		}else
		{
		query = "PhoneNumber="+phoneNumber+"&outputFormat=json";
		}
		String outPut=awsApiHttpCall(accesskey,secretkey,query,melissaurl,melissahost,melissacanonicaluri);
		return outPut;

	}
	/*
        method will call by Service Objects ReverseLookUp spark job for phone verification will pass phonenumber
         */
	public static String ServiceObjectsReverseLookUpAws(String accesskey,String secretkey,String ServiceObjectsReverseLookUpUrl,String ServiceObjectsReverseLookUphost,String ServiceObjectsReverseLookUpcanonicaluri,String phoneNumber) {
		System.out.print("melissaPhoneVerification:");
		String query="PhoneNumber="+phoneNumber+"&outputFormat=json";;
		String outPut=awsApiHttpCall(accesskey,secretkey,query,ServiceObjectsReverseLookUpUrl,ServiceObjectsReverseLookUphost,ServiceObjectsReverseLookUpcanonicaluri);
		return outPut;
	}
	/*
	method will call by clear Bit Domain Detail Request spark job for domain verification will pass domain
	 */
	public static String clearBitDomainDetailRequest(String access_key,String secret_key,String clearBitDomainUrl,String clearBitDomainhost,String clearBitDomaincanonicaluri,String website) {
		//System.out.print("zoomApiCallPerson:");
		String query="domain="+website+"&outputFormat=json";;
		String url=clearBitDomainUrl;
		String host=clearBitDomainhost;
		String canonical_uri = clearBitDomaincanonicaluri;
		String outPut=awsApiHttpCall(access_key,secret_key,query,url,host,canonical_uri);

		return outPut;
	}
	/*
	call by strikeiron spark job to validate email address
	 */
	//com.ca.ci.enrichment.AWSRequestSigner.strikeironApiEmailVerification(access_key,secret_key,StrikeironUrl,StrikeironHost,Strikeironcanonicaluri,row.getAs("email_address"),strikeironTimeout))

	public static String strikeironApiEmailVerification(String access_key,String secret_key,String StrikeironUrl,String StrikeironHost,String Strikeironcanonicaluri,String email,String strikeironTimeout) {
		//System.out.print("strikeironApiEmailVerification:");
		URLCodec codec = new URLCodec();
		String emailaddress=null;

		try {
			emailaddress=codec.encode(email);
		}
		catch(EncoderException ex)
		{
			ex.printStackTrace();

		}
		//String emailaddress=email.replace("@","%40").replace("'","%27").replace("?","%3F").replace("/","%2F").replace("+","%2B").replace("!","%21").replace("#","%23").replace("$","%24").replace("&","%26").replace("*","%2A");
		//String emailaddress=email.replace("@","%40");
		int Timeout=Integer.parseInt(strikeironTimeout);
		String query="Timeout="+Timeout+"&emailAddress="+emailaddress+"&outputFormat=json";
		//System.out.print("\nquery:"+query);
		String outPut=awsApiHttpCall(access_key,secret_key,query,StrikeironUrl,StrikeironHost,Strikeironcanonicaluri);
		return outPut;
	}


	/*
	method will call by melissa spark job for phone verification will pass phonenumber
	 */
	public static String serviceObjectPhoneVerification(String access_key,String secret_key,String clearBitDomainUrl,String clearBitDomainhost,String clearBitDomaincanonicaluri,String phoneNumber) {
		//System.out.print("serviceObjectPhoneVerification:");
		String query="PhoneNumber="+phoneNumber+"&outputFormat=json";;
		String outPut=awsApiHttpCall(access_key,secret_key,query,clearBitDomainUrl,clearBitDomainhost,clearBitDomaincanonicaluri);
		return outPut;
	}
	/*
        method will call by melissa spark job for phone verification will pass phonenumber
         */
	public static String ServiceObjectsReverseLookUpApi(String access_key,String secret_key,String ServiceObjectsReverseLookUpUrl,String ServiceObjectsReverseLookUphost,String ServiceObjectsReverseLookUpcanonicaluri,String phoneNumber) {
		//System.out.print("ServiceObjectsReverseLookUpApi");
		String query="PhoneNumber="+phoneNumber+"&TestType=FULL&outputFormat=json";;
		String outPut=awsApiHttpCall(access_key,secret_key,query,ServiceObjectsReverseLookUpUrl,ServiceObjectsReverseLookUphost,ServiceObjectsReverseLookUpcanonicaluri);
		return outPut;
	}
	/*
    method to invoke aws api with http connection and generate signature for aws signature
    */
	public static String awsApiHttpCall(String accesskeyConf,String secretkeyConf,String query,String urlPerson,String hostZoom,String canonicalurizoom)
	{
		//System.out.println("\nawsApiHttpCall query***********:\n"+query);
		String method = "GET";
		String service = "execute-api";
		String host=hostZoom;
		String endpoint=urlPerson;
		String request_parameters=query;//+"&outputFormat=json";
		String canonical_uri = canonicalurizoom;
		String region = "us-east-1";
		String access_key = accesskeyConf;
		String secret_key= secretkeyConf;
		String amzdate="";
		String datestamp = "";
		StringBuilder response = new StringBuilder();
		String jsonOut = new String();
		HttpURLConnection connection = null;
		try
		{
			Date now = new Date();
			SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
			sdf1.setTimeZone(new SimpleTimeZone(0, "UTC"));
			amzdate = sdf1.format(now);
			//System.out.println("amzdate:"+amzdate);

			SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
			sdf2.setTimeZone(new SimpleTimeZone(0, "UTC"));
			datestamp = sdf2.format(now);
			//System.out.println("datestamp:\n"+datestamp);


			String canonical_querystring = request_parameters;
			String canonical_headers = "host:" + host + "\n" + "x-amz-date:" + amzdate+"\n";
			String signed_headers = "host;x-amz-date";
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update("".getBytes("utf-8"));
			byte[] payload_hash=md.digest();
			String payload=new String(Hex.encodeHex(payload_hash));
			String canonical_request = method + "\n" + canonical_uri + "\n" + canonical_querystring+"\n" + canonical_headers +"\n" + signed_headers + "\n" +payload;
			//System.out.println("canonical_request:\n"+canonical_request);
			String algorithm = "AWS4-HMAC-SHA256";
			String credential_scope = datestamp + "/" + region + "/" + service + "/" + "aws4_request";

			//System.out.println("credential_scope:\n"+credential_scope);

			MessageDigest md2 = MessageDigest.getInstance("SHA-256");
			md2.update(canonical_request.getBytes("utf-8"));
			byte[] credential_scope_hash=md2.digest();
			String credential_scope_str=new String(Hex.encodeHex(credential_scope_hash));
			//System.out.println("credential_scope_str:\n"+credential_scope_str);


			String string_to_sign = algorithm + "\n" +  amzdate + "\n" +  credential_scope + "\n" + credential_scope_str;
			//System.out.println("string_to_sign:\n"+string_to_sign);
			byte[] signing_key = getSignatureKey(secret_key, datestamp, region, service);

			String algorithm1="HmacSHA256";
			Mac mac1 = Mac.getInstance(algorithm1);
			mac1.init(new SecretKeySpec(signing_key, algorithm));
			byte[] signing_keyhash=mac1.doFinal(string_to_sign.getBytes("utf-8"));
			String signature = new String(Hex.encodeHex(signing_keyhash));


			String authorization_header = algorithm + " " + "Credential=" + access_key + "/" + credential_scope + ", " +  "SignedHeaders=" + signed_headers + ", " + "Signature=" + signature;
			String request_url = endpoint + '?' + canonical_querystring;


			URL request_url1= new URL(request_url);

			connection = (HttpURLConnection) request_url1.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty("api-key", "338C8B47E1DEEC24595B645E1EF66458");
			connection.setRequestProperty("x-amz-date", amzdate);
			connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			connection.setRequestProperty("Host", "application/json");
			connection.setRequestProperty("Authorization",authorization_header);

			connection.setUseCaches(true);
			connection.setDoInput(true);
			connection.setDoOutput(true);

			int responcecode=connection.getResponseCode();
			/*
			responcecode 200 request sent to aws api is success
			 */
			if(responcecode==200) {
				InputStream is;
				try {
					is = connection.getInputStream();
				} catch (IOException e) {
					is = connection.getErrorStream();
				}
				//System.out.println("is\n" + is);
				String line;
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
					response.append(line);
					//response.append('\r');
				}
				jsonOut = response.toString();
				br.close();
			}else
			{
				jsonOut=null;
				//Thread.sleep(10000);
				//System.out.print("\n*********************************************************************");
				System.out.print("\nERROR:ResponseCode: "+responcecode);
				System.out.print("\nERROR:Exception for this query in AWSRequestSigner :"+request_parameters);
				System.out.print("\nERROR:request_url: "+request_url);
				//System.out.print("\n*********************************************************************");
				//throw new Exception("failed response Code: "+responsecode);
			}
			//responcecode=999;
			//System.out.println("java:"+response.toString());
		}
		catch(Exception e)
		{
			jsonOut=null;
			System.out.print("ERROR:Execption handled in awsApiHttpCall methodfor this query:"+request_parameters);
			e.printStackTrace();
		}finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
		//System.out.println("\nreturning jsonOut from AWSRequestSigner :"+jsonOut);
		return jsonOut;
	}


	
	static byte[] HmacSHA256(String data, byte[] key) throws Exception {
	    String algorithm="HmacSHA256";
	    Mac mac = Mac.getInstance(algorithm);
	    mac.init(new SecretKeySpec(key, algorithm));
	    return mac.doFinal(data.getBytes("utf-8"));
	}
	
	static byte[] getSignatureKey(String key, String dateStamp, String regionName, String serviceName) throws Exception {
	    byte[] kSecret = ("AWS4" + key).getBytes("utf-8");
	    byte[] kDate = HmacSHA256(dateStamp, kSecret);
	    byte[] kRegion = HmacSHA256(regionName, kDate);
	    byte[] kService = HmacSHA256(serviceName, kRegion);
	    byte[] kSigning = HmacSHA256("aws4_request", kService);
	    //byte[] signature = HmacSHA256(string_to_sign, kSigning);
	    return kSigning;
	}
	
	

}
