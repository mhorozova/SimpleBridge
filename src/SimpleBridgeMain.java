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
			
			// test with two XPaths
			initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-1-probe-1\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
			initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-1-probe-2\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
			
			/* all XPaths 20 000+, every XPath matches a single DataView */
			final Set<String> allXPaths = new HashSet<String>();
			
		/*
		 * For each XPath, get a list of the XPaths of all matching DataViews (0 or many)
		 */	
		for(final String path : initialXPaths){
			
			CountDownLatch latch = new CountDownLatch(1);
			
			DataViewQuery query = DataViewQuery.create(path);

	        Closable closable = conn.execute(query,
	                new Callback<DataViewChange>() {
	                    @Override
	                    public void callback(final DataViewChange change) {	                    		
	                    		
	                    	allXPaths.add(change.getSourceId());
	                    		
	                    	System.out.println(allXPaths.size() + path);
	                    		
	                    }
	                },
	                new ErrorCallback() {
	                    @Override
	                    public void error(final Exception exception) {
	                        System.err.println("Error retrieving DataSet: " + exception);
	                    }
	                }
	        );
	        
	        /* how to ensure that the query is closed and no more updates
	         * are received once we get all the matching DataViews? */
	        
	        //close the closable after e.g. 5 seconds?
			try {
				boolean result = latch.await(5, TimeUnit.SECONDS); 
				if (result) {
					System.out.println("Latch triggered - should be impossible");
				} else {
					System.out.println("Latch timed out");
				}
			} catch (InterruptedException e) {
				System.err
				.println("Interrupted exception when waiting for POPULATED");
			} finally {
				System.out.println("Finished with "+path);
				closable.close(); 
			}
	        
		}
		

		/* 3. Iterate over that list */
		// execute a query with each of these XPaths which match only 1 DataView at a time
		
//		DataViewQuery query = DataViewQuery.create(path);
//
//		conn.execute(query,
//				new Callback<DataViewChange>() {
//			@Override
//			public void callback(final DataViewChange change) {
//
//				/* 4. Write output to file */
//				System.out.println(change.getData().toString());
//				System.out.println("Data obtained at "+System.currentTimeMillis());
//
//				Writer writer = null;
//
//				try {
//					writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("outputFiles/"+path.replace('\\', '-').replace('/', '-').replace('"', ' '))));
//					writer.write(change.getData().toString());
//					writer.close();
//
//				} catch (IOException ex) {
//					// report
//				} finally {
//					try {writer.close();} catch (Exception ex) {}
//				}
//			}
//		},
//		new ErrorCallback() {
//			@Override
//			public void error(final Exception exception) {
//				System.err.println("Error retrieving DataSet: " + exception);
//			}
//		}
//				);



	}
}
