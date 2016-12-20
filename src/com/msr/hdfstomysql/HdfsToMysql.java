package com.msr.hdfstomysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.msr.hdfstomysql.HdfsMapper.Hdfsmapper;

public class HdfsToMysql 
{


	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception{
	
		Configuration conf = new Configuration();
        
        DBConfiguration.configureDB(conf,
        							"com.mysql.jdbc.Driver",
        							"jdbc:mysql://localhost:3306/edureka?autoCommit=true",
        							"root",
        							"hadoop");
		Job job = new Job(conf);
		job.setJarByClass(HdfsToMysql.class);
		job.setJobName("WordCounter");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(DBOutputFormat.class);
		
		job.setMapperClass(Hdfsmapper.class);
		job.setReducerClass(HdfsReducer.class);
	
		DBOutputFormat.setOutput( job, "emp", new String[] { "emp_no", "birth_date","first_name","last_name","gender","hire_date" });
				
		
		int returnValue = job.waitForCompletion(true) ? 0:1;
		System.out.println("job.isSuccessful "+returnValue+" " + job.isSuccessful());
	
	
}
}
