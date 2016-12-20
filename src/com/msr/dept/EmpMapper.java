package com.msr.dept;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] val = value.toString().split(",");

		int sal = Integer.parseInt(val[2]);

		context.write(new Text(val[1]), new IntWritable(sal));

	}
}
