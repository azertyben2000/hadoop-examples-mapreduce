package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class KindsCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	public int first_line = 0;
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		if (first_line != 0) {
			//Comment nous voulons un nb d'arbre nous comptons les keys
			context.write(new Text(value.toString().split(";")[3]), one);
		}
		first_line++;
	}
}