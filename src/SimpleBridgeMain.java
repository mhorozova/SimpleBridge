import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Set;
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

/**
 * @author mhorozova
 *
 */
public final class SimpleBridgeMain implements Job{

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {

		/*
		 * For each XPath, get a list of the XPaths of all matching DataViews (0 or many)
		 */	

		/* 3. Iterate over that set of XPaths */
		// execute a query with each of these XPaths which match **only 1 DataView at a time**

		executeAndWrite(Main.conn, Main.allXPaths, 4);

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
	 */
	private static void executeAndWrite(Connection conn, final Set<String> allXPaths, int timeout) {

		for(String s : allXPaths){

			DataViewQuery query = DataViewQuery.create(s);

			System.out.println("Executing a request");
			
			DataView DataView = new SimpleBridgeMain().request(conn, query, timeout, SECONDS);

			if(!DataView.equals(null)){
			try {
				Main.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("outputFiles/"+s.replace('\\', '-').replace('/', '-').replace('"', ' '))));
				System.out.println("Writing");
				try{
					Main.writer.write(DataView.toString());
					System.out.println("DataView is: "+DataView.toString());
				} catch(Exception e){
					System.out.println("Problem writing to file");
				}

				Main.writer.close();

			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				try {Main.writer.close();} catch (Exception ex) { System.out.println("Error while trying to close writer: " + ex); }
			}
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
	public DataView request(Connection conn, DataViewQuery query, long timeout, TimeUnit timeUnit) {

		System.out.println("Executing the request method");
		Closable c = conn.execute(query,
				new Callback<DataViewChange>() {
			public void callback(final DataViewChange data) {
				// mutable DataView
				Main.DataView = Main.tracker.update(data);
				//System.out.println(DataView);
				Main.cdl.countDown();
			}
		},                 new ErrorCallback() {
			@Override
			public void error(final Exception exception) {
				System.err.println("Error retrieving DataView while executing a request: " + exception);
			}
		}
				);

		try {
			Main.cdl.await(timeout, timeUnit);
			c.close();
		} catch (Exception e) {
			System.out.println("Something's wrong... Interrupted while waiting for updates");
			e.printStackTrace();
		}

		return Main.DataView;
	}
}
