import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
import com.itrsgroup.openaccess.dataview.DataViewChange;
import com.itrsgroup.openaccess.dataview.DataViewQuery;


public class RepetitiveRun implements Job{

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

			request(conn, Main.query, Main.waitInterval2, SECONDS);

			try {
				Main.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Main.outputFilesFolder+s.replace("\\", "").replace("/", "").replace("\"", ""))));
				Main.writer.write(Main.dataView.toString());
				Main.log.debug("Writing to "+s);
	
			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				try {
					Main.writer.flush();
					Main.writer.close();
										
				} catch (Exception ex) { Main.log.error("Error while trying to close writer: " + ex); }
			}

		}

		Main.log.debug("RUN FINISHED");
	}
	
	/**
	 * Executes a DataView query
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
	public static void request(Connection conn, DataViewQuery query, long timeout, TimeUnit timeUnit) {

		final CountDownLatch cdl = new CountDownLatch(1);
		//final DataViewTracker tracker = new DataViewTracker();

		Closable c = conn.execute(query,
				new Callback<DataViewChange>() {
			public void callback(final DataViewChange data) {
				// mutable DataView - now a String!
				Main.dataView = data.toString();
				cdl.countDown();
			}
		},                 new ErrorCallback() {
			@Override
			public void error(final Exception exception) {
				Main.log.error("Error retrieving DataView while executing a request: " + exception);
			}
		}
				);

		try {
			cdl.await(timeout, timeUnit);
			c.close();
		} catch (InterruptedException e) {
			Main.log.warn("Something's wrong... Interrupted while waiting for updates");
			e.printStackTrace();
		}
	}

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {

		try {
			executeWrite(Main.conn, Main.allXPaths);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
