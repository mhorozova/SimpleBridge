import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.itrsgroup.openaccess.Callback;
import com.itrsgroup.openaccess.Closable;
import com.itrsgroup.openaccess.Connection;
import com.itrsgroup.openaccess.ErrorCallback;
import com.itrsgroup.openaccess.OpenAccess;
import com.itrsgroup.openaccess.dataview.DataViewChange;
import com.itrsgroup.openaccess.dataview.DataViewQuery;

public final class SimpleBridgeMain {

	public static void main(final String[] args) {

		/* 1. Connect to cluster */
		Connection conn = OpenAccess.connect("geneos.cluster://192.168.220.41:2551?username=admin&password=admin");

		/* 2. Get a list of all DataViews */
		
		/*
		 * The customer is testing it with 10 XPaths matching 4000 DataViews
		 * but would like to subscribe to 50+ XPahts matching 20 000+ DataViews in production
		 */
		
			/* customer defined list of 50+ XPaths that match 20 000+ DataViews */
			ArrayList<String> initialXPaths = new ArrayList<String>();
			
			// test with a couple of XPaths
			/* make sure an XPath doesn't match too many DataViews
			 * as that may overload the node?*/
			initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-1-probe-1\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
			initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-1-probe-2\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
			initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-1-probe-3\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
			initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-1-probe-4\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
			initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-1-probe-5\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		//	initialXPaths.add("/geneos/gateway/directory/probe/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
			
			/* all XPaths 20 000+, every XPath matches a single DataView */
			final Set<String> allXPaths = new HashSet<String>();
			
		/*
		 * For each XPath, get a list of the XPaths of all matching DataViews (0 or many)
		 */	
		getAllMatchingDataViewsXPaths(conn, initialXPaths, allXPaths);
		
		System.out.println("Finished getting the XPaths of all matching DataViews");
		System.out.println("Proceeding with quering each one of them and writing to files");

		/* 3. Iterate over that set of XPaths */
		// execute a query with each of these XPaths which match **only 1 DataView at a time**
		
		final Iterator iterator = allXPaths.iterator();
		
		for(String xpath : allXPaths){
		
		final CountDownLatch latch = new CountDownLatch(1);
			
		DataViewQuery query = DataViewQuery.create((String) iterator.next());

		Closable closable = conn.execute(query,
				new Callback<DataViewChange>() {
			@Override
			public void callback(final DataViewChange change) {

				latch.countDown();
				/* 4. Write output to file */
				System.out.println(change.getData().toString());
				System.out.println("Data obtained at "+System.currentTimeMillis());

				Writer writer = null;

				try {
					writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("outputFiles/"+((String) iterator.next()).replace('\\', '-').replace('/', '-').replace('"', ' '))));
					writer.write(change.getData().toString());
					writer.close();

				} catch (IOException ex) {
					ex.printStackTrace();
				} finally {
					try {writer.close();} catch (Exception ex) {}
				}
			}
		},
		new ErrorCallback() {
			@Override
			public void error(final Exception exception) {
				System.err.println("Error retrieving DataSet: " + exception);
			}
		}
				);
		
        //close the query after the DataView has been queried and written to file
		try {
			latch.await(); 
		} catch (InterruptedException e) {
			System.err
			.println("Interrupted exception while waiting the latch");
		} finally {
			//System.out.println("Finished writing "+xpath);
			closable.close();
			//System.out.println("Total number of DataViews' XPaths so far: " + allXPaths.size());
		}

		}

	}

	private static void getAllMatchingDataViewsXPaths(Connection conn,
			ArrayList<String> initialXPaths, final Set<String> allXPaths) {
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
	                        System.err.println("Error retrieving DataSet: " + exception);
	                    }
	                }
	        );
	        
	        //close the query after a period of time
			try {
				latch.await(5, TimeUnit.SECONDS); 
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
