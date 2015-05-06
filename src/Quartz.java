import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

public class Quartz {

	public static void startScheduler() throws InterruptedException{
		
	    try {
	        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler(); // Grab the Scheduler instance from the Factory
	        scheduler.start(); // and start it off

	        JobDetail job = newJob(RepetitiveRun.class) // define the job and tie it to our HelloJob class
	        	//.requestRecovery()// ask scheduler to re-execute this job
	            .withIdentity("job1", "group1")
	            .build();

	        Trigger trigger = newTrigger() // Trigger the job to run now, and then repeat every x seconds
	            .withIdentity("trigger1", "group1")
	            .startNow()
	            .withSchedule(simpleSchedule()
	                    .withIntervalInSeconds(1) // how often should the job repeat (once every x seconds) - seems to be important for a job to begin once the previous one has finished
	                    .repeatForever())
	            .build();

	        scheduler.scheduleJob(job, trigger); // Tell quartz to schedule the job using our trigger
	
	        try {
				Thread.sleep(1200000000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} // how many seconds the Main should run
	        
	        scheduler.shutdown();

	    } catch (SchedulerException se) {
	        se.printStackTrace();
	    }

		}
	
}
