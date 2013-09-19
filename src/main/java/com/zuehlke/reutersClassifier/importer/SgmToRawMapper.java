package com.zuehlke.reutersClassifier.importer;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zuehlke.reutersClassifier.importer.utils.ReutersDataExtractor;

public class SgmToRawMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private StringBuilder message = new StringBuilder();
	String topics;

	private boolean isInBody = false;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if (isInBody) {
			isInBody = handleInBodyLine(context, line);
		} else {
			isInBody = handleOutOfBodyLine(line);
		}
	}

	private boolean handleInBodyLine(Context context, String line)
			throws IOException, InterruptedException {
		if (ReutersDataExtractor.isBodyEnd(line)) {
			if(StringUtils.isNotEmpty(topics)){
				storeMessage(context);
			}
			message = new StringBuilder();
			topics = null;
			return false;
		} else {
			message.append(" ").append(line);
			return true;
		}
	}

	private boolean handleOutOfBodyLine(String line) {
		if (ReutersDataExtractor.isBodyStart(line)) {
			message.append(ReutersDataExtractor.getTextAfterBodyTag(line));
			return true;
		} else {
			List<String> extractedTopics = ReutersDataExtractor.getTopics(line);
			if (!extractedTopics.isEmpty()) {
				topics = StringUtils.join(extractedTopics, ReutersDataExtractor.VALUE_DELIMITER);
			}
			return false;
		}
	}

	private void storeMessage(Context context) throws IOException, InterruptedException {
		Text messageText = new Text(topics + ReutersDataExtractor.COLUMN_DELIMITER + message);
		context.write(NullWritable.get(), messageText);
	}
}
