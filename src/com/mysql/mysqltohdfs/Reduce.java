package com.mysql.mysqltohdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.msr.inputformat.DBOutputWritable;

public class Reduce extends Reducer<Text, IntWritable, DBOutputWritable, NullWritable> {
	protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) {
		//int sum = 0;
		String line[] = key.toString().split("\t");
		try {
			ctx.write(
					new DBOutputWritable(line[0].toString(), Integer.parseInt(line[1].toString()), line[2].toString()),
					NullWritable.get());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}