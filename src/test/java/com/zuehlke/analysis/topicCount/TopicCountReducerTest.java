package com.zuehlke.analysis.topicCount;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;



public class TopicCountReducerTest {
	private static final Text KEY = new Text("category");
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	
	@Before
	public void setUp() {
		reduceDriver = ReduceDriver.newReduceDriver(new TopicCountReducer());
	}
	
	@Test
	public void output_count_one_for_one_input() throws IOException {
		List<IntWritable> counts = new LinkedList<IntWritable>();
		counts.add(new IntWritable(1));
		reduceDriver.withInput(KEY, counts);
		reduceDriver.withOutput(KEY, new IntWritable(1));

		reduceDriver.runTest();
	}
	
	@Test
	public void output_count_two_for_two_inputs() throws IOException {
		List<IntWritable> counts = new LinkedList<IntWritable>();
		counts.add(new IntWritable(1));
		counts.add(new IntWritable(1));
		reduceDriver.withInput(KEY, counts);
		reduceDriver.withOutput(KEY, new IntWritable(2));

		reduceDriver.runTest();
	}
	
}
