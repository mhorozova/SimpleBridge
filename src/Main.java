<<<<<<< HEAD
import java.io.Writer;
import java.util.ArrayList;
=======
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
>>>>>>> Timer
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

<<<<<<< HEAD
=======
import org.apache.log4j.Logger;

>>>>>>> Timer
import com.itrsgroup.openaccess.Callback;
import com.itrsgroup.openaccess.Closable;
import com.itrsgroup.openaccess.Connection;
import com.itrsgroup.openaccess.ErrorCallback;
import com.itrsgroup.openaccess.OpenAccess;
<<<<<<< HEAD
import com.itrsgroup.openaccess.dataview.DataView;
import com.itrsgroup.openaccess.dataview.DataViewChange;
import com.itrsgroup.openaccess.dataview.DataViewQuery;
import com.itrsgroup.openaccess.dataview.DataViewTracker;

public class Main {
	
	/* customer defined list of 50+ XPaths that match 20 000+ DataViews */
	public static ArrayList<String> initialXPaths;
	
	/* all XPaths 20 000+, every XPath matches a single DataView */
	public static Set<String> allXPaths;
	
	public static Connection conn;
	
	public static Writer writer;
	
	/* to be used in every new run */
	public final static DataViewTracker tracker = new DataViewTracker();
	public final static CountDownLatch cdl = new CountDownLatch(1);
	public static DataView DataView;
	
	public static void main(String[] args) throws InterruptedException{
		
		writer = null;
		
		initialXPaths = new ArrayList<String>();
		
		/* make sure an XPath doesn't match too many DataViews
		 * as that may overload the node?*/

		//initialXPaths.add("/geneos/gateway/directory/probe/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-8-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-6-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");

		
		/* 1. Connect to cluster */
		conn = OpenAccess.connect("geneos.cluster://192.168.220.41:2551?username=admin&password=admin");
		
		/* 2. Get a list of all DataViews */

		/*
		 * The customer is testing it with 10 XPaths matching 4000 DataViews
		 * but would like to subscribe to 50+ XPahts matching 20 000+ DataViews in production
		 */
		
		allXPaths = new HashSet<String>();

		getAllMatchingDataViewsXPaths(Main.conn, initialXPaths, allXPaths, 5);
		
		/* Iterate over the set of all XPaths, execute requests and write the results to files */
		
		Quartz.startScheduler();
	}
	
	/**
	 * Iterates over the customer-defined list of XPaths (expected 50+), each one of which
	 * matches 0 or many DataViews
	 * 
	 * Executes a query with each XPath and adds all matching DataViews' XPaths
	 * to the set of all XPaths
	 * 
	 * If the current query doesn't return anything for a certain amount of time
	 * (e.g. 5 seconds), the count down latch times out
	 * 
	 * @param conn
	 * @param initialXPaths
	 * @param allXPaths
	 * @param time
	 */
	static void getAllMatchingDataViewsXPaths(Connection conn,
			ArrayList<String> initialXPaths, final Set<String> allXPaths, int time) {
		for(final String path : initialXPaths){

			CountDownLatch latch = new CountDownLatch(1);

			DataViewQuery query = DataViewQuery.create(path);

			Closable closable = conn.execute(query,
					new Callback<DataViewChange>() {
				@Override
				public void callback(final DataViewChange change) {	                    		

					allXPaths.add(change.getSourceId());

				}
			},
			new ErrorCallback() {
				@Override
				public void error(final Exception exception) {
					System.err.println("Error retrieving DataView: " + exception);
				}
			}
					);

			//close the query after a period of time,
			// get all DataViews that have matched the query
			// until this path expires
			try {
				latch.await(time, TimeUnit.SECONDS); 
			} catch (InterruptedException e) {
				System.err
				.println("Interrupted exception while waiting the latch");
			} finally {
				System.out.println("Finished with "+path);
				closable.close();
				System.out.println("Total number of DataViews' XPaths so far: " + allXPaths.size());
			}

		}
	}
	
=======
import com.itrsgroup.openaccess.dataview.DataViewChange;
import com.itrsgroup.openaccess.dataview.DataViewQuery;

public class Main {
	
	static Logger log = Logger.getLogger(Main.class);
	
	static String initialPathsFile; // "initialPaths"
	static String outputFilesFolder; // "outputFiles/"
	static int repeatInterval; // 20
	static int sleepInterval; // 999999
	static int waitInterval1; // 10
	static int waitInterval2; // 2
	static String connectionDetails; // "geneos.cluster://192.168.56.101:2551?username=mhorozova&password=mhorozova"
	
	public static Connection conn;

	static Set<String> initialXPaths;
	static Set<String> allXPaths;

	 static DataViewQuery query;
	 static String dataView;

	static BufferedWriter writer;

	public static void main(String[] args) throws InterruptedException{
		
		initialPathsFile = args[0];
		outputFilesFolder = args[1];
		repeatInterval = Integer.parseInt(args[2]);
		sleepInterval = convertToMilliseconds(args[3]);
		waitInterval1 = Integer.parseInt(args[4]);
		waitInterval2 = Integer.parseInt(args[5]);
		connectionDetails = args[6];
		
		initialXPaths = new HashSet<String>();
		allXPaths = new HashSet<String>();
		writer = null;

		conn = OpenAccess.connect(connectionDetails);
		
		try {
			readPaths(initialXPaths, initialPathsFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		getAllMatchingDataViewsXPaths(conn, initialXPaths, allXPaths, waitInterval1);
	
		QuartzScheduler.startScheduler();

	}

	/**
	 * Reads customer-defined XPaths from a file and adds them in a hashset 
	 * 
	 * @param a
	 * @param fileName
	 * @throws IOException
	 */
	static void readPaths(Set<String> a, String fileName) throws IOException{
		log.info("Reading paths from "+fileName);
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {

				if(!line.startsWith("#"))
					a.add(line);

				line = br.readLine();
			}
		} finally {
			br.close();
		}
	}

	/**
	 * Iterates over the customer-defined list of XPaths (expected 50+), each one of which
	 * matches 0 or many DataViews
	 * 
	 * Executes a query with each XPath and adds all matching DataViews' XPaths
	 * to the set of all XPaths
	 * 
	 * If the current query doesn't return anything for a certain amount of time
	 * (e.g. 5 seconds), the count down latch times out
	 * 
	 * @param conn
	 * @param initialXPaths
	 * @param allXPaths
	 * @param time
	 */
	private static void getAllMatchingDataViewsXPaths(Connection conn,
			Set<String> initialXPaths, final Set<String> allXPaths, int time) {
		for(final String path : initialXPaths){

			CountDownLatch latch = new CountDownLatch(1);

			DataViewQuery query = DataViewQuery.create(path);

			Closable closable = conn.execute(query,
					new Callback<DataViewChange>() {
				@Override
				public void callback(final DataViewChange change) {	                    		

					if(!allXPaths.contains(change.getSourceId()))
						allXPaths.add(change.getSourceId());

				}
			},
			new ErrorCallback() {
				@Override
				public void error(final Exception exception) {
					log.error("Error retrieving DataView: " + exception);
				}
			}
					);

			//close the query after a period of time,
			// get all DataViews that have matched the query
			// until this path expires
			try {
				latch.await(time, TimeUnit.SECONDS); 
			} catch (InterruptedException e) {
				log.error("Interrupted exception while waiting the latch");
			} finally {
				log.debug("Finished with "+path);
				closable.close();
				log.info("Total number of XPaths matched so far: " + allXPaths.size());
			}

		}
	}
	
	static int convertToMilliseconds(String s) {
		int i = Integer.parseInt(s);
		i = i * 1000;
		return i;
	}

>>>>>>> Timer
}
