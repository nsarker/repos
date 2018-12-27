/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ivrcaller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 *
 * @author Me
 */
public class IvrCalling extends Thread {
    
    int threadId;
    int rowId;
    String callingNumber;
    DbHandler dbHandlerObj;
    
    public static void printMessage(String message)
    {
        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
	
        Calendar cal = Calendar.getInstance();
	System.out.println(dateFormat.format(cal.getTime())+" : "+message);
    }
    
    public IvrCalling(int rowId, String callingNumber, int threadId, DbHandler dbHandlerObj)
    {
        this.rowId = rowId;
        this.callingNumber = callingNumber;
        this.threadId = threadId;
        this.dbHandlerObj = dbHandlerObj;
    }
    
    @Override
    public void run() 
    {
        try 
        {
            URL url = null; 
            
            if(this.threadId%2 == 0)
            {
                url = new URL("http://127.0.0.1/phpagi/out_bound_caller_one.php?msisdn=" + this.callingNumber + "&row_id=" + this.rowId);
            }
            else
            {
                url = new URL("http://127.0.0.1/phpagi/out_bound_caller_two.php?msisdn=" + this.callingNumber + "&row_id=" + this.rowId);
            }

            printMessage(""+url);	
            
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setReadTimeout(60*1000);
            connection.connect();
            BufferedReader reader;
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            connection.disconnect();
        }
        catch (MalformedURLException ex) 
        { 
            printMessage(ex.getMessage());
            printMessage("MalformedURLException");
        } 
        catch (IOException ex) 
        {
            printMessage(ex.getMessage());
            printMessage("IOException");
        }
        
        String sql;
        sql = "UPDATE msisdn_queue_transaction SET try_count = try_count+1, last_try_time = now()";
        sql+= " WHERE id =" + this.rowId +" AND calling_num = '" + this.callingNumber + "'";
        
        this.dbHandlerObj.updateDB(sql);
        
        sql = "INSERT INTO transaction_process_history (transaction_queue_id, calling_num)";
        sql+= " VALUES(" + this.rowId+ ", '" + this.callingNumber + "')";
        
        this.dbHandlerObj.insertDB(sql);
    }    
}
