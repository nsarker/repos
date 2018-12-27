package javatest;

import java.util.Calendar;

public class jtest2 {
	public static void main(String[] args) {
		
		Calendar cal = Calendar.getInstance();
		if (cal.get(Calendar.HOUR_OF_DAY) == 20) {
			System.out.println("Calendar.HOUR_OF_DAY");
		}
		
		System.out.println(cal.get(Calendar.HOUR_OF_DAY));
	}

}
