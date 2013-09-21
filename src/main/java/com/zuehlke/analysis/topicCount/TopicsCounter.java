package com.zuehlke.analysis.topicCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopicsCounter extends Configured implements Tool  {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fileSystem = FileSystem.get(conf);
		Path input = new Path("sgmFiles");	
		Path output = new Path("counts");
		if(fileSystem.exists(output)){
			fileSystem.delete(output, true);
		}
		
		Job job = Job.getInstance(conf, "categories counter job");
		job.setJarByClass(TopicsCounter.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(TopicCountMapper.class);
		job.setReducerClass(TopicCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new TopicsCounter(), args));
	}
	
}
