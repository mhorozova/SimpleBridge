import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.itrsgroup.openaccess.Callback;
import com.itrsgroup.openaccess.Closable;
import com.itrsgroup.openaccess.Connection;
import com.itrsgroup.openaccess.ErrorCallback;
import com.itrsgroup.openaccess.OpenAccess;
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
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-1-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-1-probe-4\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-1-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-2-probe-1\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-2-probe-2\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-2-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-2-probe-4\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-2-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-3-probe-1\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-3-probe-2\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-3-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-3-probe-4\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-3-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-6-probe-1\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-6-probe-2\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-6-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-6-probe-4\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-6-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-7-probe-1\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-7-probe-2\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-7-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-7-probe-4\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-7-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-8-probe-1\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-8-probe-2\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-8-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-8-probe-4\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-8-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");

		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-9-probe-1\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-10-probe-2\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-11-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-12-probe-4\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-13-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");

		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-14-probe-1\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-15-probe-2\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-16-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-17-probe-4\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-18-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		

		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-19-probe-1\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-20-probe-2\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-21-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-22-probe-4\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-23-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-24-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-25-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		
		
		/* 1. Connect to cluster */
		conn = OpenAccess.connect("geneos.cluster://192.168.220.41:2551?username=admin&password=admin");
		
		/* 2. Get a list of all DataViews */

		/*
		 * The customer is testing it with 10 XPaths matching 4000 DataViews
		 * but would like to subscribe to 50+ XPahts matching 20 000+ DataViews in production
		 */
		
		allXPaths = new HashSet<String>();

		getAllMatchingDataViewsXPaths(Main.conn, initialXPaths, allXPaths, 20);
		
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
	
}
