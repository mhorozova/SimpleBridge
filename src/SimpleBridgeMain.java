import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.itrsgroup.openaccess.dataview.DataView;
import com.itrsgroup.openaccess.dataview.DataViewTracker;

/**
 * @author mhorozova
 *
 */
public final class SimpleBridgeMain implements Job{

	private final DataViewTracker tracker = new DataViewTracker();
	private final CountDownLatch cdl = new CountDownLatch(1);
	private DataView DataView;
	

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		

	/* all XPaths 20 000+, every XPath matches a single DataView */
		final Set<String> allXPaths = new HashSet<String>();

		/*
		 * For each XPath, get a list of the XPaths of all matching DataViews (0 or many)
		 */	
		//getAllMatchingDataViewsXPaths(Main.conn, initialXPaths, allXPaths, 5);





		/* 3. Iterate over that set of XPaths */
		// execute a query with each of these XPaths which match **only 1 DataView at a time**

		//iterateExecuteWrite(Main.conn, allXPaths);
		
		
		for(String s : Main.initialXPaths)
		System.out.println(s);
	}
//
//	/**
//	 * Iterates over the customer-defined list of XPaths (expected 50+), each one of which
//	 * matches 0 or many DataViews
//	 * 
//	 * Executes a query with each XPath and adds all matching DataViews' XPaths
//	 * to the set of all XPaths
//	 * 
//	 * If the current query doesn't return anything for a certain amount of time
//	 * (e.g. 5 seconds), the count down latch times out
//	 * 
//	 * @param conn
//	 * @param initialXPaths
//	 * @param allXPaths
//	 * @param time
//	 */
//	private static void getAllMatchingDataViewsXPaths(Connection conn,
//			ArrayList<String> initialXPaths, final Set<String> allXPaths, int time) {
//		for(final String path : initialXPaths){
//
//			CountDownLatch latch = new CountDownLatch(1);
//
//			DataViewQuery query = DataViewQuery.create(path);
//
//			Closable closable = conn.execute(query,
//					new Callback<DataViewChange>() {
//				@Override
//				public void callback(final DataViewChange change) {	                    		
//
//					allXPaths.add(change.getSourceId());
//
//				}
//			},
//			new ErrorCallback() {
//				@Override
//				public void error(final Exception exception) {
//					System.err.println("Error retrieving DataView: " + exception);
//				}
//			}
//					);
//
//			//close the query after a period of time,
//			// get all DataViews that have matched the query
//			// until this path expires
//			try {
//				latch.await(time, TimeUnit.SECONDS); 
//			} catch (InterruptedException e) {
//				System.err
//				.println("Interrupted exception while waiting the latch");
//			} finally {
//				System.out.println("Finished with "+path);
//				closable.close();
//				System.out.println("Total number of DataViews' XPaths so far: " + allXPaths.size());
//			}
//
//		}
//	}
//
//	/**
//	 * Iterates over a list of (expected
//	 *  20 000+) Strings representing all XPaths, each one of which matches a single DataView
//	 *  
//	 * Creates a query with each XPath and then executes a request with that query
//	 * (see below the request method)
//	 * 
//	 * Finally, writes the returned DataView to a file
//	 * (replaces certain characters to make it a valid filename in Windows)
//	 * 
//	 * @param conn
//	 * @param allXPaths
//	 */
//	private static void iterateExecuteWrite(Connection conn, final Set<String> allXPaths) {
//		for(String s : allXPaths){
//
//			DataViewQuery query = DataViewQuery.create(s);
//			
//			DataView DataView = new SimpleBridgeMain().request(conn, query, 2, SECONDS);
//
//			Writer writer = null;
//
//			try {
//				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("outputFiles/"+s.replace('\\', '-').replace('/', '-').replace('"', ' '))));
//				writer.write(DataView.toString());
//				writer.close();
//
//			} catch (IOException ex) {
//				ex.printStackTrace();
//			} finally {
//				try {writer.close();} catch (Exception ex) { System.out.println("Error while trying to close writer: " + ex); }
//			}
//
//		}
//		
//		System.out.println("FINISHED");
//	}
//
//	/**
//	 * Executes a DataView query and returns a DataView object 
//	 * 
//	 * If no DataViewChange is returned after a period of time
//	 * (e.g. 2 seconds) the count down latch times out
//	 * 
//	 * @param conn
//	 * @param query
//	 * @param timeout
//	 * @param timeUnit
//	 * @return
//	 */
//	public DataView request(Connection conn, DataViewQuery query, long timeout, TimeUnit timeUnit) {
//
//		Closable c = conn.execute(query,
//				new Callback<DataViewChange>() {
//			public void callback(final DataViewChange data) {
//				// mutable DataView
//				DataView = tracker.update(data);
//				//System.out.println(DataView);
//				cdl.countDown();
//			}
//		},                 new ErrorCallback() {
//			@Override
//			public void error(final Exception exception) {
//				System.err.println("Error retrieving DataView while executing a request: " + exception);
//			}
//		}
//				);
//
//		try {
//			cdl.await(timeout, timeUnit);
//			c.close();
//		} catch (InterruptedException e) {
//			System.out.println("Something's wrong... Interrupted while waiting for updates");
//			e.printStackTrace();
//		}
//
//		return DataView;
//	}
	
}
