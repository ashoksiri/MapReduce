package com.msr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

			@Override
			protected void reduce(Text key, Iterable<IntWritable> values,
					Context context)
					throws IOException, InterruptedException {
			
				int sum = 0;
				java.util.Iterator<IntWritable> valuesIt = values.iterator();
				while(valuesIt.hasNext()){
					sum = sum + valuesIt.next().get();
				}
				context.write(key, new IntWritable(sum));
			}	
}