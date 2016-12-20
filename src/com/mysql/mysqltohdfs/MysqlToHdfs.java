package com.mysql.mysqltohdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.msr.inputformat.DBInputWritable;

public class MysqlToHdfs {
	@SuppressWarnings("deprecation")
	public static void main(String[] args)
			throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		DBConfiguration.configureDB(conf, 
				"com.mysql.jdbc.Driver", // driver
				"jdbc:mysql://10.1.7.49:3306/edureka", // db url
				"root", // user name
				"hadoop"); // password
		Job job = new Job(conf);
		
		job.setJarByClass(MysqlToHdfs.class);
		job.setMapperClass(Map.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setInputFormatClass(DBInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		DBInputFormat.setInput(job, DBInputWritable.class, "employee", // input table
																	// name
				null, null, new String[] { "empid", "name", "dept" } // table
																		// columns
		);

		Path p = new Path(args[0]);
		FileSystem fs = FileSystem.get(new URI(args[0]), conf);
		fs.delete(p);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		System.out.println("Successfully Done");
	}
}
