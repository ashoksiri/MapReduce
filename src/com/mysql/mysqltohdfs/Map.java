package com.mysql.mysqltohdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.msr.inputformat.DBInputWritable;

public class Map extends Mapper<LongWritable, DBInputWritable, Text, IntWritable> {
	
	protected void map(LongWritable key, DBInputWritable value, Context ctx) {
		try {
			String name = value.getName();
			IntWritable id = new IntWritable(value.getId());
			String dept = value.getDept();
			ctx.write(new Text(name + "\t" + id + "\t" + dept), id);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}