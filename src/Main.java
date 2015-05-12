import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.itrsgroup.openaccess.Callback;
import com.itrsgroup.openaccess.Closable;
import com.itrsgroup.openaccess.Connection;
import com.itrsgroup.openaccess.ErrorCallback;
import com.itrsgroup.openaccess.OpenAccess;
import com.itrsgroup.openaccess.dataview.DataViewChange;
import com.itrsgroup.openaccess.dataview.DataViewQuery;

public class Main {

	public static Connection conn;

	static Set<String> initialXPaths;

	static Set<String> allXPaths;

	 static DataViewQuery query;
	 static String dataView;

	static BufferedWriter writer;

	public static void main(String[] args) throws InterruptedException{

		initialXPaths = new HashSet<String>();
		allXPaths = new HashSet<String>();
		writer = null;

		conn = OpenAccess.connect("geneos.cluster://192.168.220.41:2551?username=admin&password=admin");

		try {
			readPaths(initialXPaths, "initialPaths");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		getAllMatchingDataViewsXPaths(conn, initialXPaths, allXPaths, 5);
		
		TimerTask timerTask = new TimerObject();
		Timer timer = new Timer(true);
		timer.schedule(timerTask, 0, 60000);

		try {
			Thread.sleep(1200000000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Reads customer-defined XPaths from a file and adds them in a hashset 
	 * 
	 * @param a
	 * @param fileName
	 * @throws IOException
	 */
	static void readPaths(Set<String> a, String fileName) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

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

}
