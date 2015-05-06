import java.io.FileNotFoundException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.itrsgroup.openaccess.Callback;
import com.itrsgroup.openaccess.Closable;
import com.itrsgroup.openaccess.Connection;
import com.itrsgroup.openaccess.ErrorCallback;
import com.itrsgroup.openaccess.dataview.DataView;
import com.itrsgroup.openaccess.dataview.DataViewChange;
import com.itrsgroup.openaccess.dataview.DataViewQuery;
import com.itrsgroup.openaccess.dataview.DataViewTracker;

/**
 * @author mhorozova
 *
 */
public final class RepetitiveRun implements Job{
	
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {

		try {
			executeWrite(Main.conn, Main.allXPaths);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	
	/**
	 * Iterates over a list of (expected
	 *  20 000+) Strings representing all XPaths, each one of which matches a single DataView
	 *  
	 * Creates a query with each XPath and then executes a request with that query
	 * (see below the request method)
	 * 
	 * Finally, writes the returned DataView to a file
	 * (replaces certain characters to make it a valid filename in Windows)
	 * 
	 * @param conn
	 * @param allXPaths
	 * @throws FileNotFoundException 
	 */
	private static void executeWrite(Connection conn, final Set<String> allXPaths) throws FileNotFoundException {
//		for(String s : allXPaths){
//
//			Main.query = DataViewQuery.create(s);
//
//			Main.dataView = request(conn, Main.query, 2, SECONDS);
//
//			System.out.println(Main.dataView);
			
//			PrintWriter out = new PrintWriter("filename.txt");
			
//			BufferedWriter writer = null;
//
//			try {
//				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("outputFiles/"+s.replace('\\', '-').replace('/', '-').replace('"', ' '))));
//				writer.write(DataView.toString());
//				
//				writer.flush();
//				writer.close();
//				
//			} catch (IOException ex) {
//				ex.printStackTrace();
//			} finally {
//				try {
//					
//					writer.close();
//					} catch (Exception ex) { System.out.println("Error while trying to close writer: " + ex); }
//			}

//		}

		System.out.println("FINISHED");
	}

	/**
	 * Executes a DataView query and returns a DataView object 
	 * 
	 * If no DataViewChange is returned after a period of time
	 * (e.g. 2 seconds) the count down latch times out
	 * 
	 * @param conn
	 * @param query
	 * @param timeout
	 * @param timeUnit
	 * @return
	 */
	public static DataView request(Connection conn, DataViewQuery query, long timeout, TimeUnit timeUnit) {

		final CountDownLatch cdl = new CountDownLatch(1);
		final DataViewTracker tracker = new DataViewTracker();

		Closable c = conn.execute(query,
				new Callback<DataViewChange>() {
			public void callback(final DataViewChange data) {
				// mutable DataView
				Main.dataView = tracker.update(data);
				//System.out.println(DataView);
				cdl.countDown();
			}
		},                 new ErrorCallback() {
			@Override
			public void error(final Exception exception) {
				System.err.println("Error retrieving DataView while executing a request: " + exception);
			}
		}
				);

		try {
			cdl.await(timeout, timeUnit);
			c.close();
		} catch (InterruptedException e) {
			System.out.println("Something's wrong... Interrupted while waiting for updates");
			e.printStackTrace();
		}

		return Main.dataView;
	}

}
