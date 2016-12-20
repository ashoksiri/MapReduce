package com.msr.wordcount;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountMain {

	public static final boolean IBM_JAVA = true;
	
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		
		String in = "hdfs://localhost:8020/user/mapreduce/datasets/salaries.csv";
		String ou = "hdfs://localhost:8020/user/mapreduce/out";
		
		Configuration conf = new Configuration();
		//conf.set("fs.defaults.name","hdfs://localhost:8020");
		//conf.set("mapreduce.framework.name","yarn");
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCountMain.class);
		job.setJobName("WordCounter");
		
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(ou));
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
	
		FileSystem fs  = FileSystem.get(URI.create("hdfs://localhost:8020"),conf);
		
		if(fs.exists(new Path(ou)))
		{
			fs.delete(new Path(ou));
		}
		
		
		int returnValue = job.waitForCompletion(true) ? 0:1;
		System.out.println("job.isSuccessful "+returnValue+" " + job.isSuccessful());
	}
}
