package com.bigdata.analytics.bigdata;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Hello world!
 *
 */
public class App 
{
	static Configuration configuration;
	
	public App(Configuration configuration){
		this.configuration = configuration;
	}
	
	public static void main( final String[] args )
	{
		try {

			UserGroupInformation ugi  = UserGroupInformation.createRemoteUser("admin");
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					
					Path inp = new Path(args[0]);
					Path out = new Path(args[1]);
					Job job;
					job = new Job(configuration);
					job.setMapperClass(WordCountMapper.class);
					job.setReducerClass(WordCountReducer.class);
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					FileInputFormat.addInputPath((Job)job, inp);
					FileOutputFormat.setOutputPath((Job)job, out);
					job.waitForCompletion(true) ;
					return null;
				}

			});
			System.out.println("MR Job success");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch(InterruptedException ie){
			
		}

	}
}
