import java.util.ArrayList;
import java.util.List;

import com.itrsgroup.openaccess.Callback;
import com.itrsgroup.openaccess.Connection;
import com.itrsgroup.openaccess.ErrorCallback;
import com.itrsgroup.openaccess.OpenAccess;
import com.itrsgroup.openaccess.dataset.DataSetChange;
import com.itrsgroup.openaccess.dataset.DataSetItem;
import com.itrsgroup.openaccess.dataset.DataSetQuery;

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
			
			// test with a single XPath
			initialXPaths.add("/geneos/gateway/directory/probe[(@name=\"gateway-1-probe-1\")]/managedEntity/sampler[(@type=\"\")]/dataview[(@name=\"UpdateTest\")]");
		
			/* all XPaths 20 000+ */
			final ArrayList<String> allXPaths = new ArrayList<String>();
			
		/*
		 * For each XPath, get a list of all the matching DataViews (which are 0 or many),
		 * get the XPath of each one of the matching DataViews
		 */
			
		for(String path : initialXPaths){
			
			/* get the XPaths of all DataViews that match this customer defined XPath*/
			/* if the result is bigger than 128 kilobytes it will not work */
			
			DataSetQuery query = DataSetQuery.create(path);
			
	        conn.execute(query,
	                new Callback<DataSetChange>() {
	                    @Override
	                    public void callback(final DataSetChange change) {
	                    	
	                    	List<DataSetItem> matchingDataViews = new ArrayList<DataSetItem>();
	                    	
	                    		matchingDataViews = change.getItems(); 

	                    		/* add each DataView's XPath to the unified list of XPaths */
	                    		for(DataSetItem item : matchingDataViews)
	                    			allXPaths.add(item.getPath());
	                    	
	                    		// test
	                    		for(String s : allXPaths)
	                    			System.out.println(s);
	                    		
	                    }
	                },
	                new ErrorCallback() {
	                    @Override
	                    public void error(final Exception exception) {
	                        System.err.println("Error retrieving DataSet: " + exception);
	                    }
	                }
	        );
		}
		
		//final String path = "/geneos/gateway/directory/probe/managedEntity/sampler/dataview[(@name=\"LicenceUsage\")]";

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
