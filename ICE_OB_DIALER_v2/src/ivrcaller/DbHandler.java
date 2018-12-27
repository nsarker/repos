/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ivrcaller;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

/**
 *
 * @author Me
 */
public class DbHandler {
    
    private Connection conn;
    
    public static void printMessage(String message)
    {
        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
	
        Calendar cal = Calendar.getInstance();
	System.out.println(dateFormat.format(cal.getTime())+" : "+message);
    }
    
    public void createMysqlConnection() 
    {
        try 
        {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/nps", "root", "comitbd");
            printMessage("MySQL Connection Created Successfully...");
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) 
        {
            printMessage(ex.getMessage());
            printMessage("Unable to Create MySQL Connection...");
        }
    }
    
    public void closeMysqlConnection() 
    {
        if (conn != null) 
        {
            try 
            {
                conn.close();
                printMessage("MySQL Connection Closed Successfully...");
            } 
            catch (Exception ex) 
            {
                printMessage(ex.getMessage());
                printMessage("Unable to Close MySQL Connection...");
            }
        }
    }
    
    public boolean updateDB(String sql)
    {        
        try 
        {
            Statement stmt;
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
            stmt.close();
            return true;
        } 
        catch (Exception ex) 
        {
            printMessage(ex.getMessage());
            return false;
        }
    }
    
    public ResultSet getResult(String sql) 
    {        
        try 
        {
            Statement stmt;
            stmt = conn.createStatement();
            ResultSet rset = stmt.executeQuery(sql);
            return rset;
        } 
        catch (Exception ex) 
        {
            printMessage(ex.getMessage());
            return null;
        }
    }
    
    public boolean insertDB(String sql)
    {
        try 
        {
            Statement stmt;
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
            stmt.close();
            return true;
        } 
        catch (Exception ex) 
        {
            printMessage(ex.getMessage());
            return false;
        }
    }
    
    public boolean batchInsertDeleteDB(ArrayList<String> queries)
    {
        try 
        {
            Statement stmt;
            stmt = conn.createStatement();

            for (String query : queries) 
            {
                stmt.addBatch(query);
            }

            stmt.executeBatch();
            stmt.close();
            
            return true;
        }
        catch (Exception ex) 
        {
            printMessage(ex.getMessage());
            return false;
        }
    }
}