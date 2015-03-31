import com.itrsgroup.openaccess.Callback;
import com.itrsgroup.openaccess.Connection;
import com.itrsgroup.openaccess.ErrorCallback;
import com.itrsgroup.openaccess.OpenAccess;
import com.itrsgroup.openaccess.dataview.DataViewChange;
import com.itrsgroup.openaccess.dataview.DataViewQuery;

public final class SimpleBridgeMain {
	
    public static void main(final String[] args) {
    	
    	/* 1. Connect to cluster*/
    	Connection conn = OpenAccess.connect("geneos.cluster://192.168.10.205:2551?username=admin&password=admin");

    	/* 2. Get list of all DataViews*/
    	String path = "/geneos/gateway/directory/probe/managedEntity/sampler/dataview[(@name=\"LicenceUsage\")]";
    	
    	/* 3. Iterate over that list*/
    	DataViewQuery query = DataViewQuery.create(path);
    	
        conn.execute(query,
                new Callback<DataViewChange>() {
                    @Override
                    public void callback(final DataViewChange change) {
        
                    	/* 4. Write output to file*/
                    	System.out.println(change);
                    	System.out.println("Data obtained at "+System.currentTimeMillis());
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
}
