package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class KindsMapper extends Mapper<Object, Text, Text, NullWritable> {
	public int first_line = 0;

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		if (first_line != 0) {
			//Set keys NullWritable car nous ne cherchons que les types
			context.write(new Text(value.toString().split(";")[3]), NullWritable.get());
		}
		first_line++;
	}
}