package com.msr.partitioner;

import java.io.IOException;
//import java.util.StringTokenizer;
//
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartitionerMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// sample record
		// nameagegendersalary
		// Raju 23 male5000
		// Rani 21 female 50000
		String[] tokens = value.toString().split("\t");
		String gender = tokens[2].toString();
		String nameAgeSalary = tokens[0] + "\t" + tokens[1] + "\t" + tokens[3];
		// the mapper emits key, value pair where the key is the gender and the
		// value is the other information which includes name, age and score
		context.write(new Text(gender), new Text(nameAgeSalary));
	}
}