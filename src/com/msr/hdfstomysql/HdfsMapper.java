package com.msr.hdfstomysql;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class HdfsMapper {

	static class Hdfsmapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		 
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			
				context.write(value,NullWritable.get());
			
		}
	}
}