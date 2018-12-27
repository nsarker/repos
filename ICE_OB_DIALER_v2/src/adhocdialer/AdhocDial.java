package adhocdialer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

public class AdhocDial {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
    public static String msisdnMakeFormate(String msisdn) {
        msisdn = msisdn.trim();
        
        if (msisdn.length() == 13 && "88018".equals(msisdn.substring(0, 5))) {
//            return msisdn.substring(0, 2);
            return msisdn.substring(2, msisdn.length()-1);
        } 
        else if (msisdn.length() == 14 && "+88018".equals(msisdn.substring(0, 6))) {
//            return msisdn.substring(0, 4);
            return msisdn.substring( 3, msisdn.length()-1);
        } 
        else if (msisdn.length() == 15 && "0088018".equals(msisdn.substring(0, 7))) {
//            return msisdn.substring(0, 4);
            return msisdn.substring(4, msisdn.length()-1);
        } 
        else if (msisdn.length() == 11 && "018".equals(msisdn.substring(0, 3))) {
            return msisdn;
        } 
        else if (msisdn.length() == 10 && "18".equals(msisdn.substring(0, 2))) {
            return "0"+msisdn;
        } 
        else {
            return "NO";
        }
    }

}
