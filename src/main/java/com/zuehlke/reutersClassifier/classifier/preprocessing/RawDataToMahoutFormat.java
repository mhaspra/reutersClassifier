package com.zuehlke.reutersClassifier.classifier.preprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RawDataToMahoutFormat extends Configured implements Tool   {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fileSystem = FileSystem.get(conf);
		Path input = new Path("rawFiles");
		Path output = new Path("mahoutSequenceFiles");
		
		if(fileSystem.exists(output)){
			fileSystem.delete(output, true);
		}
		
		Job job = Job.getInstance(conf, "Raw sequence file to mahout format");
		job.setJarByClass(RawDataToMahoutFormat.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(RawDataToMahoutFormatMapper.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new RawDataToMahoutFormat(), args));
	}
}
