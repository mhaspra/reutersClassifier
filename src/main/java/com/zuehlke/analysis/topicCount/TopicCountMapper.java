package com.zuehlke.analysis.topicCount;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zuehlke.reutersClassifier.importer.utils.ReutersDataExtractor;

public class TopicCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		List<String> topics = ReutersDataExtractor.getTopics((value.toString()));
		if(!topics.isEmpty()){
			for(String topic : topics){
				context.write(new Text(topic), new IntWritable(1));
			}
		}
	}
}
