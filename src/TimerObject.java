import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.itrsgroup.openaccess.Callback;
import com.itrsgroup.openaccess.Closable;
import com.itrsgroup.openaccess.Connection;
import com.itrsgroup.openaccess.ErrorCallback;
import com.itrsgroup.openaccess.dataview.DataViewChange;
import com.itrsgroup.openaccess.dataview.DataViewQuery;


public class TimerObject extends TimerTask {

	@Override
	public void run() {
		
		System.out.println("Timer task started at:"+new Date());

		try {
			executeWrite(Main.conn, Main.allXPaths);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//completeTask();

		System.out.println("Timer task finished at:"+new Date());
		
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

		for(String s : allXPaths){

			Main.query = DataViewQuery.create(s);

			Main.dataView = request(conn, Main.query, 2, SECONDS);

			try {
				Main.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("outputFiles/"+s.replace('\\', '-').replace('/', '-').replace('"', ' '))));
				Main.writer.write(Main.dataView.toString());
				System.out.println("Writing to "+s);

				Main.writer.flush();
				Main.writer.close();

			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				try {

					Main.writer.close();
				} catch (Exception ex) { System.out.println("Error while trying to close writer: " + ex); }
			}

		}

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
	public static String request(Connection conn, DataViewQuery query, long timeout, TimeUnit timeUnit) {

		final CountDownLatch cdl = new CountDownLatch(1);
		//final DataViewTracker tracker = new DataViewTracker();

		Closable c = conn.execute(query,
				new Callback<DataViewChange>() {
			public void callback(final DataViewChange data) {
				// mutable DataView - now a String!
				Main.dataView = data.toString();
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
