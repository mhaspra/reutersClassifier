package com.zuehlke.reutersClassifier.importer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SgmToRawFile extends Configured implements Tool  {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fileSystem = FileSystem.get(conf);
		Path input = new Path("sgmFiles");	
		Path output = new Path("rawFiles");
		if(fileSystem.exists(output)){
			fileSystem.delete(output, true);
		}
		
		Job job = Job.getInstance(conf, "Sgm files to raw file");
		job.setJarByClass(SgmToRawFile.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(SgmToRawMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SgmToRawFile(), args));
	}
	
}
