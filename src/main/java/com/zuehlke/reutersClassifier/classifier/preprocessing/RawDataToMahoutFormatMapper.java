package com.zuehlke.reutersClassifier.classifier.preprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zuehlke.reutersClassifier.importer.utils.ReutersDataExtractor;

public class RawDataToMahoutFormatMapper  extends Mapper<LongWritable, Text, Text, Text> {
	private Set<String> validTopics = new HashSet<String>();
	
	@Override
	public void setup(Context context) throws IOException {
		File file = new File("topiclist");
		if(file.exists() )
		{
			FileReader fileReader = null;
			try {
				fileReader = new FileReader( file );
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			BufferedReader reader = new BufferedReader(fileReader);
			String topicLine = reader.readLine();
			while(topicLine != null){
				validTopics.add(topicLine);
				topicLine = reader.readLine();
			}
		}
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String message = extractMessage(line);
		for(String topic: extractTopics(line)){
			if(validTopics.contains(topic)){
				context.write(new Text("/" + topic + "/" + key.get()), new Text(message));
			}
		}
	}

	private String extractMessage(String line) {
		return StringUtils.substringAfter(line, ReutersDataExtractor.COLUMN_DELIMITER);
	}

	private String[] extractTopics(String line) {
		String topics = StringUtils.substringBefore(line, ReutersDataExtractor.COLUMN_DELIMITER);
		return topics.split(ReutersDataExtractor.VALUE_DELIMITER);
	}
}
