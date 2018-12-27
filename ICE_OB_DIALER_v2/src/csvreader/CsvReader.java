/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package csvreader;

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

/**
 *
 * @author Me
 */
public class CsvReader {

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
    
    
    public static int csv2db()
    {
        int sl;
        sl = 1;
        String csvFileName = getFileName();
        System.out.println(csvFileName);
        //String csvFile = "G://JAVA Projects/combined/"+csvFileName;
        String csvFile = "/var/www/html/nps/FTP/combined/"+csvFileName;
        
        //NS:  ArrayList
        ArrayList<String> uniqueNumberList = new ArrayList<String>();
        uniqueNumberList.clear();

        File f = new File(csvFile);

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
            
            sql = "TRUNCATE TABLE msisdn_queue_master_temp";
            printMessage(sql);
            dbHandlerObj.updateDB(sql);
            
            ////////////////////////////////////////////////////////
            //NS: delete yesterday record from unqiue caller
            
            //old delete query
            //sql = "delete from tbl_msisdn WHERE insertdt < DATE(NOW())";
    		Calendar cal = Calendar.getInstance();
    		if (cal.get(Calendar.HOUR_OF_DAY) == 10) {
    			System.out.println("since hh is 10, hence truncate tbl_msisdn table");
    			sql = "TRUNCATE TABLE tbl_msisdn";
                printMessage(sql);
                dbHandlerObj.updateDB(sql);
    		}
    		

            
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
                printMessage("No Data Found to Display at tbl_msisdn...");
            }
            // END //////////////////////////////////////////////////////

            try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

                while ((line = br.readLine()) != null) {
                    String[] data = line.split(cvsSplitBy);
                    //System.out.println(sl+") "+data[0]);
                    //sl++;
                    if (data.length < 17) {
                    	printMessage("Unwanted format of data...");
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
                        //System.out.println(callingNum+" already in database...");
                        continue;
                    }
                    
                    //NS: check with daily-unique-msisdn-table 
                    if(uniqueNumberList.contains(callingNum))
                    {
//                        System.out.println(callingNum+" already in Daily Unique database...");
                        continue;
                    }
                    
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

                    sql = "INSERT INTO msisdn_queue_master_temp (ucid, call_id, date, start_time, end_time, duration_in_seec, ";
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
                sl--;
                printMessage("Finally "+ sl + " Number of Rows Inserted Successfully...");
            }

            dbHandlerObj.closeMysqlConnection();
        } else {
        	printMessage(csvFileName + " Not Found...");
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
                
                printMessage("dailyNumberCount: "+ dailyNumberCount);

            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            printMessage("No Data Found to Display...");
        }
        
        maxEvc = (int) (dailyNumberCount / 100) * 7;
        maxHvc = (int) (dailyNumberCount / 100) * 3;
        maxMass = (int) (dailyNumberCount / 100) * 90;
        
        // Added by Nasir Sarker
        ////// By passing Shift Percentage //
        
        String skillCategory;
        String sql2insert;
        String sql2unique= "";
        int rowId;
        evc = 0;
        hvc = 0;
        mass = 0;
        
        sql = "SELECT id, skill_categorized FROM msisdn_queue_master_temp WHERE date(date)=date(now()-INTERVAL 0 DAY)";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM dnd_list WHERE status = 1)";
        
        rset = dbHandlerObj.getResult(sql);
        try {
        	 while (rset.next()) {
                 rowId = rset.getInt(1);
                 skillCategory = rset.getString(2);
//                 System.out.println("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);

                 if("EVC".equals(skillCategory) && evc < maxEvc)
                 {
//                     sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                	 sql2insert = "INSERT INTO msisdn_queue_master (ucid, call_id, date, start_time, end_time, duration_in_seec, dialed_num, calling_num, vdn, frl, ocs, agent_id, file_name, insert_date, skill, skill_categorized, queue_time, ";
                	 sql2insert += "talk_time, hold_time, acw_time, consult_time, station, cdr_file_name) ";
                	 sql2insert += "SELECT ucid, call_id, date, start_time, end_time, duration_in_seec, dialed_num, calling_num, vdn, frl, ocs, agent_id, file_name, insert_date, skill, skill_categorized, queue_time, ";
                	 sql2insert += "talk_time, hold_time, acw_time, consult_time, station, cdr_file_name FROM msisdn_queue_master_temp WHERE id ="+rowId;
//                     printMessage(sql2insert);
                     dbHandlerObj.updateDB(sql2insert);
                     //NS: Insert to daily-unique-table
                     sql2unique = "INSERT INTO tbl_msisdn (msisdn) SELECT calling_num FROM msisdn_queue_master_temp WHERE id ="+rowId;
//                     printMessage(sql2unique);
                     dbHandlerObj.insertDB(sql2unique);
                     
                     evc++;
                     totalEvc++;
//                     System.out.println("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);
                 }
                 else if("HVC".equals(skillCategory) && hvc < maxHvc)
                 {
//                     sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                     sql2insert = "INSERT INTO msisdn_queue_master (ucid, call_id, date, start_time, end_time, duration_in_seec, dialed_num, calling_num, vdn, frl, ocs, agent_id, file_name, insert_date, skill, skill_categorized, queue_time, ";
                     sql2insert += "talk_time, hold_time, acw_time, consult_time, station, cdr_file_name) ";
                     sql2insert += "SELECT ucid, call_id, date, start_time, end_time, duration_in_seec, dialed_num, calling_num, vdn, frl, ocs, agent_id, file_name, insert_date, skill, skill_categorized, queue_time, ";
                     sql2insert += "talk_time, hold_time, acw_time, consult_time, station, cdr_file_name FROM msisdn_queue_master_temp WHERE id ="+rowId;
//                     printMessage(sql2insert);
                     dbHandlerObj.updateDB(sql2insert);
                     //NS: Insert to daily-unique-table
                     sql2unique = "INSERT INTO tbl_msisdn (msisdn) SELECT calling_num FROM msisdn_queue_master_temp WHERE id ="+rowId;
//                     printMessage(sql2unique);
                     dbHandlerObj.insertDB(sql2unique);
                     
                      hvc++;
                     totalHvc++;
//                     System.out.println("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);
                 }
                 else if("Mass".equals(skillCategory) && mass < maxMass)
                 {
//                     sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                	 sql2insert = "INSERT INTO msisdn_queue_master (ucid, call_id, date, start_time, end_time, duration_in_seec, dialed_num, calling_num, vdn, frl, ocs, agent_id, file_name, insert_date, skill, skill_categorized, queue_time, ";
                	 sql2insert += "talk_time, hold_time, acw_time, consult_time, station, cdr_file_name) ";
                	 sql2insert += "SELECT ucid, call_id, date, start_time, end_time, duration_in_seec, dialed_num, calling_num, vdn, frl, ocs, agent_id, file_name, insert_date, skill, skill_categorized, queue_time, ";
                	 sql2insert += "talk_time, hold_time, acw_time, consult_time, station, cdr_file_name FROM msisdn_queue_master_temp WHERE id ="+rowId;
//                     printMessage(sql2insert);
                     dbHandlerObj.updateDB(sql2insert);
                     //NS: Insert to daily-unique-table
                     sql2unique = "INSERT INTO tbl_msisdn (msisdn) SELECT calling_num FROM msisdn_queue_master_temp WHERE id ="+rowId;
//                     printMessage(sql2unique);
                     dbHandlerObj.insertDB(sql2unique);
                     
                     mass++;
                     totalMass++;
//                     printMessage("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);
                 }
                 else
                 {
                     //System.out.println("Nothing to do....");
                 }
        	 }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            printMessage("No Data Found ...");
        }
        
        printMessage("Total EVC: "+evc+", HVC: "+hvc+", Mass: "+mass+" for inserted");
        
        
        // NS: More Information
        int totalMQM_inserted = evc+hvc+mass;
        printMessage("Total Insertion into msisdn_queue_master (EVC+HVC+Mass): "+totalMQM_inserted);

        sql = "select count(1) as cnt from msisdn_queue_master_temp where date(now() ) = date(master_entry_time)\n" + 
        		"and  hour(now() ) = hour(master_entry_time)";
        rset = dbHandlerObj.getResult(sql);
        
        try {
        	if(rset.next())
	        {
	            int totalMQM_inserted2=rset.getInt("cnt");
	            printMessage("Total Insertion into msisdn_queue_master (from DB): "+totalMQM_inserted2);
	        }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            printMessage("No Data Found to Display...");
        }


        
        
        
        /*
        
        ////////////
        morningShiftTotalCount = (int)((dailyNumberCount/100)*morningShiftPercentage);
        afternoonShiftTotalCount = (int)(dailyNumberCount/100)*afternoonShiftPercentage;
        eveningShiftTotalCount = (int)(dailyNumberCount/100)*eveningShiftPercentage;
        nightShiftTotalCount = (int)(dailyNumberCount/100)*nightShiftPercentage;
        
        msEvc = (int)(morningShiftTotalCount/100)*evcPercentage;
        msHvc = (int)(morningShiftTotalCount/100)*hvcPercentage;
        msMass = (int)(morningShiftTotalCount/100)*massPercentage;
        asEvc = (int)(afternoonShiftTotalCount/100)*evcPercentage;
        asHvc = (int)(afternoonShiftTotalCount/100)*hvcPercentage;
        asMass = (int)(afternoonShiftTotalCount/100)*massPercentage;
        esEvc = (int)(eveningShiftTotalCount/100)*evcPercentage;
        esHvc = (int)(eveningShiftTotalCount/100)*hvcPercentage;
        esMass = (int)(eveningShiftTotalCount/100)*massPercentage;
        nsEvc = (int)(nightShiftTotalCount/100)*evcPercentage;
        nsHvc = (int)(nightShiftTotalCount/100)*hvcPercentage;
        nsMass = (int)(nightShiftTotalCount/100)*massPercentage;
        
        System.out.println("MS EVC: "+msEvc);
        System.out.println("MS HVC: "+msHvc);
        System.out.println("MS Mass: "+msMass);
        
        System.out.println("AS EVC: "+asEvc);
        System.out.println("AS HVC: "+asHvc);
        System.out.println("AS Mass: "+asMass);
        
        System.out.println("ES EVC: "+esEvc);
        System.out.println("ES HVC: "+esHvc);
        System.out.println("ES Mass: "+esMass);
        
        System.out.println("NS EVC: "+nsEvc);
        System.out.println("NS HVC: "+nsHvc);
        System.out.println("NS Mass: "+nsMass);
        
        String skillCategory;
        String sql2insert;
        int rowId;
        
      //  *****MORNING SHIFT******
        evc = 0;
        hvc = 0;
        mass = 0;
        
        sql = "SELECT id, skill_categorized FROM msisdn_queue_master_temp WHERE date(date)=date(now()-INTERVAL 1 DAY)";
        sql+= " AND start_time >= '06:00' AND start_time <= '11:59'";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM dnd_list WHERE status = 1)";
        
        System.out.println("\n\n-------------Morning Shift-------------------");
        
        rset = dbHandlerObj.getResult(sql);
        
        try {
            while (rset.next()) {
                rowId = rset.getInt(1);
                skillCategory = rset.getString(2);
                
                //System.out.println(rowId+" = "+skillCategory);
                
                System.out.println("Morning Shift => EVC: "+evc+", HVC: "+hvc+", Mass: "+mass);
                System.out.println("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);
                        
                if("EVC".equals(skillCategory) && evc < msEvc)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    evc++;
                    totalEvc++;
                }
                else if("HVC".equals(skillCategory) && hvc < msHvc)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    hvc++;
                    totalHvc++;
                }
                else if("Mass".equals(skillCategory) && mass < msMass)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    mass++;
                    totalMass++;
                }
                else
                {
                    //System.out.println("Nothing to do....");
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println("No Data Found for Morning Shift...");
        }
        
       // *****AFTERNOON SHIFT******
        evc = 0;
        hvc = 0;
        mass = 0;
        
        sql = "SELECT id, skill_categorized FROM msisdn_queue_master_temp WHERE date(date)=date(now()-INTERVAL 1 DAY)";
        sql+= " AND start_time >= '12:00' AND start_time <= '17:59'";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM dnd_list WHERE status = 1)";
        
        System.out.println("\n\n-------------Afternoon Shift-------------------");
        
        rset = dbHandlerObj.getResult(sql);
        
        try {
            while (rset.next()) {
                rowId = rset.getInt(1);
                skillCategory = rset.getString(2);
				
                System.out.println("Afternoon Shift => EVC: "+evc+", HVC: "+hvc+", Mass: "+mass);
                System.out.println("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);
                        
                if("EVC".equals(skillCategory) && evc < asEvc)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    evc++;
                    totalEvc++;
                }
                else if("HVC".equals(skillCategory) && hvc < asHvc)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    hvc++;
                    totalHvc++;
                }
                else if("Mass".equals(skillCategory) && mass < asMass)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    mass++;
                    totalMass++;
                }
                else
                {
                    //System.out.println("Nothing to do....");
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println("No Data Found for Afternoon Shift...");
        }
        
        System.out.println("Total EVC: "+evc+", HVC: "+hvc+", Mass: "+mass+" for Afternoon Shift");
        
       // *****EVENING SHIFT******
        evc = 0;
        hvc = 0;
        mass = 0;
        
        sql = "SELECT id, skill_categorized FROM msisdn_queue_master_temp WHERE date(date)=date(now()-INTERVAL 1 DAY)";
        sql+= " AND start_time >= '18:00' AND start_time <= '23:59'";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM dnd_list WHERE status = 1)";
        
        System.out.println("\n\n-------------Evening Shift-------------------");
        
        rset = dbHandlerObj.getResult(sql);
        
        try {
            while (rset.next()) {
                rowId = rset.getInt(1);
                skillCategory = rset.getString(2);
				
		System.out.println("Evening Shift => EVC: "+evc+", HVC: "+hvc+", Mass: "+mass);
                System.out.println("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);
                        
                if("EVC".equals(skillCategory) && evc < esEvc)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    evc++;
                    totalEvc++;
                }
                else if("HVC".equals(skillCategory) && hvc < esHvc)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    hvc++;
                    totalHvc++;
                }
                else if("Mass".equals(skillCategory) && mass < esMass)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    mass++;
                    totalMass++;
                }
                else
                {
                    //System.out.println("Nothing to do....");
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println("No Data Found for Evening Shift...");
        }
        
        System.out.println("Total EVC: "+evc+", HVC: "+hvc+", Mass: "+mass+" for Evening Shift");
        
        // *****NIGHT SHIFT******
        evc = 0;
        hvc = 0;
        mass = 0;
        
        sql = "SELECT id, skill_categorized FROM msisdn_queue_master_temp WHERE date(date)=date(now()-INTERVAL 1 DAY)";
        sql+= " AND start_time >= '00:00' AND start_time <= '05:59'";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM dnd_list WHERE status = 1)";
        
        System.out.println("\n\n-------------Night Shift-------------------");
        
        rset = dbHandlerObj.getResult(sql);
        
        try {
            while (rset.next()) {
                rowId = rset.getInt(1);
                skillCategory = rset.getString(2);
				
		System.out.println("Night Shift => EVC: "+evc+", HVC: "+hvc+", Mass: "+mass);
                System.out.println("Total => EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass);
                        
                if("EVC".equals(skillCategory) && evc < nsEvc)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    evc++;
                    totalEvc++;
                }
                else if("HVC".equals(skillCategory) && hvc < nsHvc)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    hvc++;
                    totalHvc++;
                }
                else if("Mass".equals(skillCategory) && mass < nsMass)
                {
                    sql2insert = "INSERT INTO msisdn_queue_master SELECT * FROM msisdn_queue_master_temp WHERE id ="+rowId;
                    
                    dbHandlerObj.updateDB(sql2insert);
                    mass++;
                    totalMass++;
                }
                else
                {
                    //System.out.println("Nothing to do....");
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println("No Data Found for Night Shift...");
        }
        
        System.out.println("Total EVC: "+totalEvc+", HVC: "+totalHvc+", Mass: "+totalMass+" for Night Shift");
        
        ////////////// - shift logic ends
        
        sql = " INSERT INTO msisdn_queue_master";
        sql+= " SELECT * FROM msisdn_queue_master_temp WHERE skill_categorized = 'EVC'";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM msisdn_queue_master)";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM dnd_list WHERE status = 1)";
        sql+= " LIMIT "+(maxEvc-totalEvc);

	System.out.println(sql);
        
        dbHandlerObj.updateDB(sql);
        
        sql = " INSERT INTO msisdn_queue_master";
        sql+= " SELECT * FROM msisdn_queue_master_temp WHERE skill_categorized = 'HVC'";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM msisdn_queue_master)";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM dnd_list WHERE status = 1)";
        sql+= " LIMIT "+(maxHvc-totalHvc);

	System.out.println(sql);
        
        dbHandlerObj.updateDB(sql);
        
        sql = " INSERT INTO msisdn_queue_master";
        sql+= " SELECT * FROM msisdn_queue_master_temp WHERE skill_categorized = 'Mass'";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM msisdn_queue_master)";
        sql+= " AND calling_num NOT IN (SELECT calling_num FROM dnd_list WHERE status = 1)";
        sql+= " LIMIT "+(maxMass-totalMass);

	System.out.println(sql);
        
        dbHandlerObj.updateDB(sql);     
   
    
    */
        
        
    }
    
    public static void printMessage(String message)
    {
        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
	
        Calendar cal = Calendar.getInstance();
	System.out.println(dateFormat.format(cal.getTime())+" : "+message);
    }

    public static void main(String[] args) {
    	printMessage("=== Starting CsvReader ===");
        int totalInsertedRows;
        totalInsertedRows = csv2db();
        
        if(totalInsertedRows > 0)
        {
            //System.out.println("Total "+totalInsertedRows+" Rows Inserted...");
        	printMessage("Total "+totalInsertedRows+" Rows Inserted...");

            temp2master();
        }
        
//        System.out.println("Exiting from CsvReader...");
        printMessage("Exiting from CsvReader...");
        System.out.println("");
    }

}
