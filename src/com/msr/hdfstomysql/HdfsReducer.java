
package com.msr.hdfstomysql;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.msr.inputformat.EmployeeOutputWritable;

class HdfsReducer extends Reducer<Text, NullWritable, EmployeeOutputWritable, NullWritable> {

	@Override
			protected void reduce(Text key, Iterable<NullWritable> values,
					Context context)
					throws IOException, InterruptedException {
			
				String line[] = key.toString().split(",");
				
				 context.write(new EmployeeOutputWritable(Integer.parseInt(line[0].toString()),line[1].toString(),line[2].toString(),line[3].toString(),line[4].toString(),line[5].toString()), NullWritable.get());
			}
}