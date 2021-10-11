package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class HeightOfKindsMapper extends Mapper<Object, Text, Text, FloatWritable> {
	public int first_line = 0;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (first_line != 0) {
			try {
				//Stockage d'un key composer du district et de la taille des arbres
				Float height = Float.parseFloat(value.toString().split(";")[6]);
				context.write(new Text(value.toString().split(";")[3]), new FloatWritable(height));
			} catch (NumberFormatException error) {
				// Si error de la methode parseFloat()
				System.out.print("Error parseFloat : " + error.getMessage());
			}
		} first_line++;
	}
}