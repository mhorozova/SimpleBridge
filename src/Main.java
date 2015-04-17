import com.itrsgroup.openaccess.Connection;
import com.itrsgroup.openaccess.OpenAccess;

public class Main {

	public static Connection conn;
	
	public static void main(String[] args) throws InterruptedException{
		
		/* 1. Connect to cluster */
		conn = OpenAccess.connect("geneos.cluster://192.168.220.41:2551?username=admin&password=admin");
		
		Quartz.startScheduler();
	}
	
}