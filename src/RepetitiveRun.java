import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.itrsgroup.openaccess.Callback;
import com.itrsgroup.openaccess.Closable;
import com.itrsgroup.openaccess.Connection;
import com.itrsgroup.openaccess.ErrorCallback;
import com.itrsgroup.openaccess.dataview.DataView;
import com.itrsgroup.openaccess.dataview.DataViewCell;
import com.itrsgroup.openaccess.dataview.DataViewChange;
import com.itrsgroup.openaccess.dataview.DataViewHeadline;
import com.itrsgroup.openaccess.dataview.DataViewQuery;
import com.itrsgroup.openaccess.dataview.DataViewRow;


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
				Main.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Main.outputFilesFolder+setOutputFilesFormat(s))));
				Main.log.debug("Writing to "+Main.outputFilesFolder+setOutputFilesFormat(s));
				Main.writer.write(Main.dataView);

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
				//Main.dataView = data.toString();
				
				// need to change the file contents format though...
				DataView dv = Main.tracker.update(data);
				Main.dataView = setContentsFormat(dv);
				
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

	public static String setOutputFilesFormat(String a){

		String b = "";

		Pattern p = Pattern.compile(Pattern.quote("[") + "(.*?)" + Pattern.quote("]"));
		Matcher m = p.matcher(a);

		while (m.find()){
			b = b + "[" + betweenStrings(m.group(1))+"]";
		}

		if(!b.isEmpty())
			return b;
		else
			return "[]";
	}

	/**
	 * Returns the String between two quotes - e.g. "this is returned"
	 * If there are more than one things between Strings it should return them separated by commas
	 * 
	 * e.g. " "123""456""789" "
	 * should be returned as 123,456,789
	 * 
	 * @param s
	 * @return
	 */
	public static String betweenStrings(String s){

		String bQuotes = "";
		String lastMatch = "";

		Pattern pQuotes = Pattern.compile(Pattern.quote("\"") + "(.*?)" + Pattern.quote("\""));

		Matcher mQuotes = pQuotes.matcher(s);
		Matcher mReverse = pQuotes.matcher(new StringBuilder(s).reverse());

		if(mReverse.find())
			lastMatch = new StringBuilder(mReverse.group(1)).reverse().toString();

		while(mQuotes.find()){

			if(!lastMatch.equals(mQuotes.group(1))) // if it is not the last match
				bQuotes = bQuotes + mQuotes.group(1) + ",";
			else
				bQuotes = bQuotes + mQuotes.group(1);
		}

		return bQuotes;

	}

	public static String setContentsFormat(DataView dv){
  		String s = "";
		
		List<DataViewHeadline> headlines = dv.getHeadlines();
		String rowHeader = dv.getRowHeader();
		List<String> columnHeaders = dv.getColumnHeaders();
		List<DataViewRow> rows = dv.getRows();

		s = s + ("id="+setOutputFilesFormat(dv.getId())+"\n");                

		for(DataViewHeadline headline : headlines)
			s = s + (headline.getName()+"="+headline.getValue()+"\n");

		s = s + "\n";

		s = s + (rowHeader + "\01");

		for(String columnHeader : columnHeaders)
			s = s + (columnHeader+"\01");

		s = s + "\n";

		for(DataViewRow row : rows){
			s = s + row.getName()+"\01";
			List<DataViewCell> cells = row.getCells();
			for(DataViewCell cell : cells)
				s = s + (cell.getValue()+"\01");
			s = s + "\n";
			
		}
		return s;
	}
}
