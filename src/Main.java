
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.itrsgroup.openaccess.Callback;
import com.itrsgroup.openaccess.Closable;
import com.itrsgroup.openaccess.Connection;
import com.itrsgroup.openaccess.ErrorCallback;
import com.itrsgroup.openaccess.OpenAccess;
import com.itrsgroup.openaccess.dataview.DataViewChange;
import com.itrsgroup.openaccess.dataview.DataViewQuery;
import com.itrsgroup.openaccess.dataview.DataViewTracker;

public class Main {
	
    static Logger log = LoggerFactory.getLogger(Main.class);
	
	static String initialPathsFile; // "initialPaths"
	static String outputFilesFolder; // "outputFiles/"
	static int repeatInterval; // 20
	static int waitInterval1; // 10
	static int waitInterval2; // 2
	static String connectionDetails; // "geneos.cluster://192.168.56.101:2551?username=mhorozova&password=mhorozova"
	
    // Declare a tracker outside of the callback
    final static DataViewTracker tracker = new DataViewTracker();
	
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
		waitInterval1 = Integer.parseInt(args[3]);
		waitInterval2 = Integer.parseInt(args[4]);
		connectionDetails = args[5];
		
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
	 * Iterates through the customer-defined list of XPaths (expected 50+), each one of which
	 * matches 0 or many DataViews
	 * 
	 * Executes a query with each XPath and adds all matching DataViews' XPaths
	 * to the set allXPaths
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
			
			Main.log.info("Checking matching XPaths for "+path);

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

}
