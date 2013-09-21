package com.zuehlke.analysis.topicCount;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class TopicCountMapperTest {

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

	@Before
	public void setUp() {
		mapDriver = MapDriver.newMapDriver(new TopicCountMapper());
	}
	
	@Test
	public void no_output_for_empty_topic_line() throws IOException {
		setUpMapDriverInput("no_topic.txt");
		
		mapDriver.runTest();
	}

	@Test
	public void one_output_for_one_topic() throws IOException{
		setUpMapDriverInput("one_topic.txt");
		mapDriver.withOutput(new Text("cocoa"), new IntWritable(1));
		
		mapDriver.runTest();
	}
	
	@Test
	public void two_outputs_for_two_topics() throws IOException{
		setUpMapDriverInput("two_topics.txt");
		mapDriver.withOutput(new Text("cocoa"), new IntWritable(1));
		mapDriver.withOutput(new Text("corn"), new IntWritable(1));
		
		mapDriver.runTest();
	}
	

	private void setUpMapDriverInput(String filename) throws IOException {
		LineIterator lineIterator1 = FileUtils.lineIterator(FileUtils.toFile(this.getClass().getResource(filename)));
		LineIterator lineIterator = lineIterator1;
		while (lineIterator.hasNext()) {
			mapDriver.withInput(new LongWritable(1), new Text(lineIterator.next()));
		}
	}
}
