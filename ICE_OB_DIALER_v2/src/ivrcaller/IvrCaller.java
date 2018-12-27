/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ivrcaller;

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
public class IvrCaller {
    
    public static void printMessage(String message)
    {
        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
	
        Calendar cal = Calendar.getInstance();
	System.out.println(dateFormat.format(cal.getTime())+" : "+message);
    }
    
    public static boolean checkMaxHourPassed()
    {
        Calendar presentDateTime = Calendar.getInstance();
	
        Calendar maxTime = Calendar.getInstance();
        maxTime.set(Calendar.HOUR_OF_DAY, 19);
        maxTime.set(Calendar.MINUTE, 55);
        maxTime.set(Calendar.SECOND, 0);
        
        if(presentDateTime.after(maxTime)) 
        {
            return true;
        } 
        
        return false;
    }

    public static void main(String[] args) throws SQLException {
     
        while(true)
        {
            DbHandler dbHandlerObj = new DbHandler();        
            dbHandlerObj.createMysqlConnection();

            int dailyNumberCount;
            dailyNumberCount = 0;
            int tryCount;
            tryCount = 0;
            int threadCount;
            threadCount = 0;
            String sql;
            ResultSet rset;
            ArrayList<String> insertQueries;
            insertQueries = new ArrayList<>();
                    
            ArrayList<String> deleteQueries;
            deleteQueries = new ArrayList<>();
            
            sql = "SELECT daily_number_count, daily_try_count, thread_count FROM ivr_settings";
            rset =  dbHandlerObj.getResult(sql);
            
            try 
            {
                while (rset.next())
                {
                    dailyNumberCount = rset.getInt(1);
                    tryCount = rset.getInt(2);
                    threadCount = rset.getInt(3);
                }
            }
            catch (SQLException ex) 
            {
                printMessage(ex.getMessage());
                printMessage("No Data Found to Display...");
            } 
            
            
            
            if(tryCount > 0)
            {            
                sql = "SELECT id FROM msisdn_queue_transaction WHERE ";
                sql+= "answer_status = 1 OR try_count >= "+tryCount+" OR DATE(date) < DATE(NOW()-INTERVAL 0 DAY)";
                
                rset =  dbHandlerObj.getResult(sql);
                
                try 
                {
                    insertQueries.clear();
                    deleteQueries.clear();
                    
                    while (rset.next())
                    { 
                        int row_id;
                        row_id = 0;
                       
                        row_id = rset.getInt(1);
                        
                        sql = "INSERT INTO msisdn_queue_transaction_dump (id, ucid, call_id, date, start_time, ";
                        sql+= "end_time, duration_in_seec, dialed_num, calling_num, vdn, frl, ocs, agent_id, ";
                        sql+= "file_name, insert_date, skill, skill_categorized, queue_time, talk_time, hold_time, ";
                        sql+= "acw_time, consult_time, station, master_entry_time, transaction_entry_time, ";
                        sql+= "try_count, last_try_time, answer_status, cdr_file_name) SELECT  id, ucid, call_id, date, ";
                        sql+= "start_time, end_time, duration_in_seec, dialed_num, calling_num, vdn, frl, ocs, ";
                        sql+= "agent_id, file_name, insert_date, skill, skill_categorized, queue_time, talk_time, ";
                        sql+= "hold_time, acw_time, consult_time, station, master_entry_time, transaction_entry_time, ";
                        sql+= "try_count, last_try_time, answer_status, cdr_file_name FROM msisdn_queue_transaction WHERE ";
                        sql+= "id = "+row_id;
                        
                        insertQueries.add(sql);
                        
                        sql = "DELETE FROM msisdn_queue_transaction WHERE id = "+row_id;
                            
                        deleteQueries.add(sql);
                    }
                    
                    if(insertQueries.size() > 0)
                    {
                        if(dbHandlerObj.batchInsertDeleteDB(insertQueries))
                        {
                            printMessage(insertQueries.size()+" Number Successfully Inserted to Transaction Dump Table...");

                            if(dbHandlerObj.batchInsertDeleteDB(deleteQueries))
                            {
                                printMessage(deleteQueries.size()+" Number Successfully Deleted from Transaction Table...");
                            }
                        }
                    }
                }
                catch (SQLException ex) 
                {
                    printMessage(ex.getMessage());    
                    printMessage("No Number Found to Move in Transaction Dump...");
                }  //NS:: completed: msisdn_queue_transaction > msisdn_queue_transaction_dump
                
                
                
                sql = "SELECT COUNT(*) as TOTAL_NUMBER FROM (";
                sql+= "SELECT calling_num FROM msisdn_queue_transaction ";
                sql+= "WHERE date(transaction_entry_time) = date(now()) UNION ALL ";
                sql+= "SELECT calling_num FROM msisdn_queue_transaction_dump ";
                sql+= "WHERE date(transaction_entry_time) = date(now()) ";
                sql+= ") AS TEMP_TABLE";
                
                rset =  dbHandlerObj.getResult(sql);
                
                int totalTransactionToday;
                totalTransactionToday = 0;
                
                try 
                {
                    while (rset.next())
                    {
                        totalTransactionToday = rset.getInt(1);
                    }                 
                }   
                catch (SQLException ex) 
                {
                    printMessage(ex.getMessage());
                    printMessage("No Number has been Processed Today (till now)...");
                }
                
                printMessage("Total "+totalTransactionToday+" Number has been Uploded in Transaction Table for Processing...");
                
                if(totalTransactionToday == 0)
                {
                    sql = "INSERT INTO msisdn_queue_master_dump SELECT * FROM msisdn_queue_master ";
                    sql+= " WHERE DATE(date)<date(NOW()-INTERVAL 0 DAY)";
                    
                    if(dbHandlerObj.updateDB(sql))
                    {
                        printMessage("Previous Dated Calls Successfully Inserted in Master Dump Table...");
                        
                        sql = "DELETE FROM msisdn_queue_master WHERE DATE(date)<date(NOW()-INTERVAL 0 DAY)";
                        
                        if(dbHandlerObj.updateDB(sql))
                        {
                            printMessage("Previous Dated Calls Successfully Deleted from Master Table...");
                        }
                        else
                        {
                            printMessage("Unable to Delete Previous Dated Calls from Master Table...");
                        }
                    }
                    else 
                    {
                        printMessage("Unable to Insert Previous Dated Calls in Master Dump Table...");
                    }
                }  //NS:: completed: msisdn_queue_master > msisdn_queue_master_dump

                
                
                
                int limit;
                limit = dailyNumberCount - totalTransactionToday;
                
                if((dailyNumberCount - totalTransactionToday) > threadCount)
                {
                    limit = threadCount;
                }
                
                
                if(totalTransactionToday >= dailyNumberCount)
                {
                    limit = 0;
                    
                    sql = "SELECT COUNT(calling_num) as total_calling_num FROM msisdn_queue_transaction_dump ";
                    sql+= "WHERE date(transaction_entry_time) = date(now())";
                    
                    rset =  dbHandlerObj.getResult(sql);
                
                    int totalDumpToday;
                    totalDumpToday = 0;
                
                    try 
                    {
                        while (rset.next())
                        {
                            totalDumpToday = rset.getInt(1);
                        }                 
                    }   
                    catch (SQLException ex) 
                    {
                        printMessage(ex.getMessage());
                        printMessage("Unable to Read Data from Transaction Dump...");
                    }
                    
                    if(totalDumpToday >= dailyNumberCount)
                    {
                        printMessage("Daily Maximum Quata "+dailyNumberCount+" Completed...");
                        printMessage("oopS!!! ... Exiting Process...");
                        dbHandlerObj.closeMysqlConnection();
                        printMessage("Bye...");                    
                        break;
                    }
                    else
                    {
                        printMessage("Number Limit "+totalDumpToday+" to Move to Transaction is over...");
                    }
                }
                else if(checkMaxHourPassed())
                {
                    printMessage("Daily Maximum Process Time 8:00pm Passed...");
                    printMessage("oopS!!! ... Exiting Process...");
                    dbHandlerObj.closeMysqlConnection();
                    printMessage("Bye...");                    
                    break;
                }    
                
                
                if(limit > 0)
                {
                    sql = "SELECT id FROM msisdn_queue_master ORDER BY id LIMIT "+limit;                    

                    printMessage("Looking to Select "+limit+" Number from Master Table...");

                    rset =  dbHandlerObj.getResult(sql);

                    try 
                    {
                        insertQueries.clear();
                        deleteQueries.clear();

                        while (rset.next())
                        { 
                            int row_id;
                            row_id = 0;

                            row_id = rset.getInt(1);

                            sql = "INSERT INTO msisdn_queue_transaction ( ucid, call_id, date, start_time, ";
                            sql+= "end_time, duration_in_seec, dialed_num, calling_num, vdn, frl, ocs, agent_id, ";
                            sql+= "file_name, insert_date, skill, skill_categorized, queue_time, talk_time, hold_time, ";
                            sql+= "acw_time, consult_time, station, master_entry_time, cdr_file_name) SELECT  ucid, call_id, date, ";
                            sql+= "start_time, end_time, duration_in_seec, dialed_num, calling_num, vdn, frl, ocs, ";
                            sql+= "agent_id, file_name, insert_date, skill, skill_categorized, queue_time, talk_time, ";
                            sql+= "hold_time, acw_time, consult_time, station, master_entry_time, cdr_file_name FROM ";
                            sql+= "msisdn_queue_master WHERE id ="+row_id;

                            insertQueries.add(sql);

                            sql = "DELETE FROM msisdn_queue_master WHERE id = "+row_id;

                            deleteQueries.add(sql);
                        }

                        if(insertQueries.size() > 0)
                        {
                            if(dbHandlerObj.batchInsertDeleteDB(insertQueries))
                            {
                                printMessage(insertQueries.size()+" Number Successfully Inserted to Transaction Table...");

                                if(dbHandlerObj.batchInsertDeleteDB(deleteQueries))
                                {
                                    printMessage(deleteQueries.size()+" Numnber Successfully Deleted from Master Table...");
                                }
                            }
                        }
                    }
                    catch (SQLException ex) 
                    {
                        printMessage(ex.getMessage());
                        printMessage("No Data Found to add on Transaction Table...");
                    }
                } // take data to call MQM>MQT
            }  // end if block of trycount>0 
            
            
            
            
            
            System.out.println(threadCount+"+"+tryCount);
            
            if(threadCount > 0 && tryCount > 0)
            {
                sql = "SELECT id, calling_num FROM msisdn_queue_transaction ";
                sql+= " WHERE try_count < "+ tryCount + " AND answer_status != 1";
                sql+= " AND last_try_time < DATE_SUB(NOW(), INTERVAL 1 HOUR)";
                sql+= " ORDER BY try_count ASC, id ASC LIMIT " + threadCount;
                System.out.println(sql);
                rset =  dbHandlerObj.getResult(sql);
                
                try 
                {
                    ArrayList<IvrCalling> ivrCallingList;
                    ivrCallingList = new ArrayList<>();
                    int i;
                    i = 0;

                    while (rset.next())
                    {            
                        IvrCalling request = new IvrCalling(rset.getInt(1), rset.getString(2), i, dbHandlerObj);
                        request.start();
                        ivrCallingList.add(request);
                     
                        i++;
                    }

                    for(i = 0; i < ivrCallingList.size(); i++)
                    {
                        ivrCallingList.get(i).join();
                    }
                    
                    printMessage(""+i+" Number of Call has been Attempted Successfully...");
                } 
                catch (SQLException ex) 
                {
                    printMessage(ex.getMessage());
                    printMessage("No Data Found to Display...");
                } 
                catch (InterruptedException ex) 
                {
                    printMessage(ex.getMessage());
                    printMessage("Unable to Join Threads...");
                }               

                try 
                {
                	//printMessage("Sleeping for 120 Seconds...");
                    printMessage("Sleeping for 60 Seconds...");
                    Thread.sleep(60000);//1000 milliseconds is one second.
                } 
                catch(InterruptedException ex) 
                {
                    printMessage(ex.getMessage());
                    printMessage("Unable to Sleep for 60 Seconds...");
                }
                
                dbHandlerObj.closeMysqlConnection();
                
                try 
                {
                    printMessage("Sleeping for 30 Seconds...");
                    Thread.sleep(30000);//1000 milliseconds is one second.
                } 
                catch(InterruptedException ex) 
                {
                    printMessage(ex.getMessage());
                    printMessage("Unable to Sleep for 30 Seconds...");
                }
            }
            else
            {
                printMessage("Daily Configured Try Count : " + tryCount + " & Thread Count : " + threadCount);
                printMessage("oopS!!! Nothing to do... Exiting Process...");
                dbHandlerObj.closeMysqlConnection();
                printMessage("Bye...");                    
                break;
            }
        
        } // end of first while
    }    
}
