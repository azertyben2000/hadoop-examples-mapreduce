package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class DistrictMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	public int first_line = 0;
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		if (first_line != 0) {
			context.write(new IntWritable(Integer.parseInt(value.toString().split(";")[1])), one);
		}
		first_line++;
	}
}