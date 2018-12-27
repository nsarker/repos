package javatest;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import csvreader.DbHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

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

public class JavaTest {

	public static void main(String[] args) {
		
		
		
    	printMessage("=== Starting TEST CsvReader ===");
        for(int i = 0; i < args.length; i++) {
            System.out.println("args: "+args[i]);
        }   
        
        int totalInsertedRows;
        totalInsertedRows = csv2db(args[0]);
        
        if(totalInsertedRows > 0)
        {
        	printMessage("Total "+totalInsertedRows+" Rows Inserted to temp tbl...");
            temp2master();
        }
        printMessage("Exiting from CsvReader...");
        System.out.println("");
        
      
        
        

	}
	
	
	
	
	
	

    public static String getFileName() {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

        Calendar cal = Calendar.getInstance();
        
        //no need to subtract 1 day
        //cal.add(Calendar.DATE, 0);
        cal.add(Calendar.HOUR, -1);

//        return "cdr_ivr_data_" + dateFormat.format(cal.getTime()) + "235959.csv";
        //return "cdr_data_20161114135959.csv";
        
        //Added by Nasir Sarker 18-Nov-18
        DateFormat dateFormatHour = new SimpleDateFormat("HH");
        return "CDR_Report_" + dateFormat.format(cal.getTime()) +"_"+ dateFormatHour.format(cal.getTime())+"00.csv";
        // new format  CDR_Report_20181118_1900.csv


    }

    // Fixed by Nasir for 1st 3 cases
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
    

    
    public static int csv2db(String csvFileName)
    {
        int sl;
        sl = 1;
//        String csvFileName = getFileName();
        System.out.println(csvFileName);
        //String csvFile = "G://JAVA Projects/combined/"+csvFileName;
        String csvFile = "/var/www/html/nps/FTP/combined/"+csvFileName;

        File f = new File(csvFile);

        //NS:  ArrayList
        ArrayList<String> uniqueNumberList = new ArrayList<String>();
        uniqueNumberList.clear();
        

        // tests if file exists
        if (f.exists()) {
            String line = "";
            String cvsSplitBy = ",";
            int i;
            i = 1;
            String sql;
            ResultSet rset;
            ArrayList<String> numberList = new ArrayList<String>();
            ArrayList<String> batchInsert = new ArrayList<String>();

            DbHandler dbHandlerObj = new DbHandler();
            dbHandlerObj.createMysqlConnection();
            
            sql = "TRUNCATE TABLE X_msisdn_queue_master_temp";
            printMessage(sql);
            dbHandlerObj.updateDB(sql);
            
            ////////////////////////////////////////////////////////
            //NS: delete yesterday record from unqiue caller
            sql = "delete from tbl_msisdn WHERE insertdt < DATE(NOW())";
            printMessage(sql);
            dbHandlerObj.updateDB(sql);
            
            //NS: get unique table to List
            sql = "SELECT distinct msisdn FROM tbl_msisdn";
            printMessage(sql);
            rset = dbHandlerObj.getResult(sql);
            
            try {
                while (rset.next()) {
                	uniqueNumberList.add(rset.getString(1));
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
                System.out.println("No Data Found to Display...");
            }
            // END //////////////////////////////////////////////////////

            
            

            try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

                while ((line = br.readLine()) != null) {
                    String[] data = line.split(cvsSplitBy);
                    //System.out.println(sl+") "+data[0]);
                    //sl++;
                    if (data.length < 17) {
                        System.out.println("Unwanted format of data...");
                        continue;
                    }
                    DateFormat dateFormat = new SimpleDateFormat("yyyy-m-d");
                    String ucId = data[0];
                    String callId = data[1];
                    String dateString = data[2];

                    String[] tempDate = dateString.split("-");
                    String date = "";

                    for (i = (tempDate.length) - 1; i >= 0; i--) {
                        if (tempDate[i] != null) {
                            date += tempDate[i] + '-';
                        }
                    }

                    String startTime = data[3];
                    String endTime = data[4];
                    String durationInSec = data[5];
                    String DialedNum = data[6];
                    String callingNum = data[7];
                    callingNum = msisdnMakeFormate(callingNum);
                    
                    if("NO".equals(callingNum))
                    {
                        //System.out.println(callingNum+""+data[7]);
                        continue;
                    }
                    
                    if(numberList.indexOf(callingNum)!= -1)
                    {
                        System.out.println(callingNum+" already in CSV...");
                        continue;
                    }
                    
                    //NS: check with daily unique call list 
                    if(uniqueNumberList.contains(callingNum))
                    {
                        System.out.println(callingNum+" already in Daily Unique database...");
                        continue;
                    }
                    
//                    uniqueNumberList.
                    
                    String vdn = data[8];
                    String frl = data[9];
                    String ocs = data[10];

		    if(!"answered".equals(ocs.trim().toLowerCase()))
                    {
                        continue;
                    }

                    String agentId = data[11];
                    String fileName = data[12];
                    String insertDateString = data[13];

                    String[] tempInsertDate = insertDateString.split("-");
                    String insertDate = "";

                    for (i = (tempInsertDate.length) - 1; i >= 0; i--) {
                        if (tempInsertDate[i] != null) {
                            insertDate += tempInsertDate[i] + "-";
                        }
                    }

                    String skill = data[14];
                    String skillCategorized = skill;

                    switch (skill) {
                        case "GPRS_Bng_P1":
                        case "GPRS_Eng_P1":
                        case "Pos_Bng_P1":
                        case "Pos_Eng_P1":
                        case "Pre_Bng_P1":
                        case "Pre_Eng_P1":
                            skillCategorized = "HVC";
                            break;
                        case "Corporate":
                        case "General_Bangla":
                        case "General_English":
                        case "GPRS_Bng_P2":
                        case "GPRS_Bng_P3":
                        case "GPRS_Eng_P2":
                        case "GPRS_Eng_P3":
                        case "IR":
                        case "Pos_Bng_P2":
                        case "Pos_Bng_P3":
                        case "Pos_Eng_P2":
                        case "Pos_Eng_P3":
                        case "Pre_Bng_P2":
                        case "Pre_Bng_P3":
                        case "Pre_Eng_P2":
                        case "Pre_Eng_P3":
                        case "RSP":
                            skillCategorized = "Mass";
                            break;
                    }

                    String queueTime = data[15];
                    String talkTime = data[16];
                    String holdTime = data[17];
                    String acwTime = data[18];
                    String consultTime = data[19];
                    String station = null;

                    if (data.length > 20) {
                        station = data[20];
                    }

                    sql = "INSERT INTO X_msisdn_queue_master_temp (ucid, call_id, date, start_time, end_time, duration_in_seec, ";
                    sql += "dialed_num, calling_num, vdn, frl, ocs, agent_id, file_name, insert_date, skill, skill_categorized, queue_time, ";
                    sql += "talk_time, hold_time, acw_time, consult_time, station, cdr_file_name) VALUES ('" + ucId + "', '" + callId + "', '" + date + "', ";
                    sql += "'" + startTime + "', '" + endTime + "', '" + durationInSec + "', '" + DialedNum + "', '" + callingNum + "', '" + vdn + "', '" + frl + "', ";
                    sql += "'" + ocs + "', '" + agentId + "', '" + fileName + "', '" + insertDate + "', '" + skill + "', '" + skillCategorized + "', '" + queueTime + "', '" + talkTime + "', ";
                    sql += "'" + holdTime + "', '" + acwTime + "', '" + consultTime + "', '" + station + "', '" + csvFileName + "')";
                    
                    batchInsert.add(sql);
                    
                    if(batchInsert.size() > 999)
                    {
                        dbHandlerObj.batchInsertDeleteDB(batchInsert);
                        batchInsert = new ArrayList<String>();
                        printMessage(sl + " Number of Rows Inserted Successfully...");
                    }
                    
                    numberList.add(callingNum);

                    sl++;
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally{
                dbHandlerObj.batchInsertDeleteDB(batchInsert);
                System.out.println("Finally "+ sl + " Number of Rows Inserted Successfully...");
            }

            dbHandlerObj.closeMysqlConnection();
        } else {
            System.out.println(csvFileName + " Not Found...");
        }
        
        return sl;
    }
    
    public static void temp2master()
    {
        int morningShiftPercentage;
        int afternoonShiftPercentage;
        int eveningShiftPercentage;
        int nightShiftPercentage;
        int dailyNumberCount;
        int morningShiftTotalCount;
        int afternoonShiftTotalCount;
        int eveningShiftTotalCount;
        int nightShiftTotalCount;
        int evcPercentage;
        int hvcPercentage;
        int massPercentage;
        int msEvc;
        int msHvc;
        int msMass;
        int asEvc;
        int asHvc;
        int asMass;
        int esEvc;
        int esHvc;
        int esMass;
        int nsEvc;
        int nsHvc;
        int nsMass;
        int totalEvc;
        int totalHvc;
        int totalMass;
        int evc;
        int hvc;
        int mass;
        int maxEvc;
        int maxHvc;
        int maxMass;

        String sql;
        ResultSet rset;
        
        morningShiftPercentage = 30;	//old value 35
        afternoonShiftPercentage = 30;	//old value 20
        eveningShiftPercentage = 30;	//old value 40
        nightShiftPercentage = 10;	//old value 5
        evcPercentage = 7;
        hvcPercentage = 3;
        massPercentage = 90;
        dailyNumberCount = 0;
        totalEvc = 0;
        totalHvc = 0;
        totalMass = 0;
        
        sql = "";
        
        DbHandler dbHandlerObj = new DbHandler();
        dbHandlerObj.createMysqlConnection();

//        sql = "SELECT daily_number_count FROM ivr_settings";
        sql = "SELECT daily_number_count, hourlyfile_number_count FROM ivr_settings";
        rset = dbHandlerObj.getResult(sql);

        try {
            while (rset.next()) {
                dailyNumberCount = rset.getInt(1);
                
                // Added by Nasir Sarker.
                // Development value. comment on production
//                dailyNumberCount = 2500;  // hard coded
                dailyNumberCount = rset.getInt(2);  // take data from hourlyfile_number_count
                
                System.out.println("dailyNumberCount: "+ dailyNumberCount);

            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println("No Data Found to Display...");
        }
        
        maxEvc = (int) (dailyNumberCount / 100) * 7;
        maxHvc = (int) (dailyNumberCount / 100) * 3;
        maxMass = (int) (dailyNumberCount / 100) * 90;
        
        // Added by Nasir Sarker
        ////// By passing Shift Percentage //
        
        String skillCategory;
        String sql2insert;
        int rowId;
        evc = 0;
        hvc = 0;
        mass = 0;
        
        sql = "SELECT id, skill_categorized FROM X_msisdn_queue_master_temp WHERE date(date)=date(now()-INTERVAL 0 DAY)";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM dnd_list WHERE status = 1)";
        
        String sql2unique= "";
        		
        rset = dbHandlerObj.getResult(sql);
        try {
        	 while (rset.next()) {
                 rowId = rset.getInt(1);
                 skillCategory = rset.getString(2);
//                 System.out.println("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);

                 if("EVC".equals(skillCategory) && evc < maxEvc)
                 {
                     sql2insert = "INSERT INTO X_msisdn_queue_master SELECT * FROM X_msisdn_queue_master_temp WHERE id ="+rowId;
                     dbHandlerObj.updateDB(sql2insert);
                     
                     //NS: Insert to Unique table
                     sql2unique = "INSERT INTO tbl_msisdn (msisdn) SELECT calling_num FROM X_msisdn_queue_master_temp WHERE id ="+rowId;
                     dbHandlerObj.insertDB(sql2unique);
                     
                     evc++;
                     totalEvc++;
//                     System.out.println("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);
                 }
                 else if("HVC".equals(skillCategory) && hvc < maxHvc)
                 {
                     sql2insert = "INSERT INTO X_msisdn_queue_master SELECT * FROM X_msisdn_queue_master_temp WHERE id ="+rowId;
                     dbHandlerObj.updateDB(sql2insert);

                     //NS: Insert to Unique table
                     sql2unique = "INSERT INTO tbl_msisdn (msisdn) SELECT calling_num FROM X_msisdn_queue_master_temp WHERE id ="+rowId;
                     dbHandlerObj.insertDB(sql2unique);
                     
                     hvc++;
                     totalHvc++;
//                     System.out.println("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);
                 }
                 else if("Mass".equals(skillCategory) && mass < maxMass)
                 {
                     sql2insert = "INSERT INTO X_msisdn_queue_master SELECT * FROM X_msisdn_queue_master_temp WHERE id ="+rowId;
                     dbHandlerObj.updateDB(sql2insert);

                     //NS: Insert to Unique table
                     sql2unique = "INSERT INTO tbl_msisdn (msisdn) SELECT calling_num FROM X_msisdn_queue_master_temp WHERE id ="+rowId;
                     dbHandlerObj.insertDB(sql2unique);
                     
                     mass++;
                     totalMass++;
//                     System.out.println("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);
                 }
                 else
                 {
                     //System.out.println("Nothing to do....");
                 }
        	 }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println("No Data Found ...");
        }
        
        printMessage("Total EVC: "+evc+", HVC: "+hvc+", Mass: "+mass+" for inserted");

       
        
    }
    
    public static void printMessage(String message)
    {
        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
	
        Calendar cal = Calendar.getInstance();
	System.out.println(dateFormat.format(cal.getTime())+" : "+message);
    }


}
