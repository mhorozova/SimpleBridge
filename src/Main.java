import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.itrsgroup.openaccess.Connection;

public class Main {

	public static Connection conn;
	
	static ArrayList<String> initialXPaths;
	
	public static void main(String[] args) throws InterruptedException{
		
		/* 1. Connect to cluster */
		//conn = OpenAccess.connect("geneos.cluster://192.168.220.41:2551?username=admin&password=admin");
		
		

		/* 2. Get a list of all DataViews */

		/*
		 * The customer is testing it with 10 XPaths matching 4000 DataViews
		 * but would like to subscribe to 50+ XPahts matching 20 000+ DataViews in production
		 */

		/* customer defined list of 50+ XPaths that match 20 000+ DataViews */

		/* make sure an XPath doesn't match too many DataViews
		 * as that may overload the node?*/
		initialXPaths = new ArrayList<String>();
		
		/* test reading paths from file */
		try {
			readPaths(initialXPaths);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Quartz.startScheduler();
	}
	
	static void readPaths(ArrayList<String> a) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader("initialPaths"));
	    try {
	        StringBuilder sb = new StringBuilder();
	        String line = br.readLine();
	        a.add(line);
	        while (line != null) {
	        	
	        	a.add(line);
	        	
	            line = br.readLine();
	        }
	        String everything = sb.toString();
	        System.out.println(everything);
	    } finally {
	        br.close();
	    }
	}
	
}
