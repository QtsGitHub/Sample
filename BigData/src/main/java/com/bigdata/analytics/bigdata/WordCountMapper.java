package com.bigdata.analytics.bigdata;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public  class WordCountMapper extends Mapper <Object, Text, Text, IntWritable>{


	@Override
	protected void map(Object key, Text value,Context context)	throws IOException, InterruptedException {
		 StringTokenizer strTokenizer = new StringTokenizer(value.toString());
         Text token = new Text();
         
         IntWritable one = new IntWritable(1);
         while (strTokenizer.hasMoreTokens()) {
             token.set(strTokenizer.nextToken());
             context.write(token, one);
         }
	}
	
}
