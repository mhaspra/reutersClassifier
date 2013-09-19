package com.zuehlke.reutersClassifier.classifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.naivebayes.test.TestNaiveBayesDriver;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.utils.SplitInput;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

import com.zuehlke.reutersClassifier.classifier.preprocessing.RawDataToMahoutFormat;


public class ReutersClassifierRunner extends Configured implements Tool  {
	private static final String TEMP_DIR = "tmp";
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fileSystem = FileSystem.get(conf);
		Path tempDir = new Path(TEMP_DIR);
		String outputDir = "result";
		
		RawDataToMahoutFormat fileSplitter = new RawDataToMahoutFormat();
		fileSplitter.setConf(conf);
		
		int exitCode = fileSplitter.run(null);
				if(exitCode != 0){
			fileSystem.delete(tempDir, true);
			return exitCode;
		}
		
		
		
		
		
		Path mahoutSequenceFiles = new Path("mahoutSequenceFiles");	
		Path extracted = new Path(TEMP_DIR, "extracted");	
		
		SparseVectorsFromSequenceFiles sparseVectorsFromSequenceFiles = new SparseVectorsFromSequenceFiles();
		sparseVectorsFromSequenceFiles.setConf(conf);
		exitCode = sparseVectorsFromSequenceFiles.run(new String[]{"-i", mahoutSequenceFiles.toString(), "-o", extracted.toString(), "-lnorm", "-wt", "tfidf" });
		if(exitCode != 0){
			fileSystem.delete(tempDir, true);
			return exitCode;
		}
		
		Path tfidf = new Path(TEMP_DIR, "extracted/tfidf-vectors");
		Path training = new Path(outputDir, "training");
		Path test = new Path(outputDir, "testing");
		SplitInput splitInput = new SplitInput();
		splitInput.setConf(conf);
		exitCode = splitInput.run(new String[]{"-i", tfidf.toString(), 
											   "--trainingOutput", training.toString(), 
											   "--testOutput", test.toString(), 
											   "--randomSelectionPct", "40",
											   "--overwrite", 
											   "--sequenceFiles",
											   "-xm", "sequential" });
		if(exitCode != 0){
			fileSystem.delete(tempDir, true);
			return exitCode;
		}
		
		Path model = new Path(outputDir, "model");
		Path labels = new Path(outputDir, "labels");		
		Path bayesTempDir = new Path(TEMP_DIR, "bayes");
		TrainNaiveBayesJob trainBayes = new TrainNaiveBayesJob();
		trainBayes.setConf(conf);		
		exitCode = trainBayes.run(new String[]{"-i", training.toString(), "-o", model.toString(), "-li", labels.toString(), "--tempDir", bayesTempDir.toString(), "-ow", "-el" });
		if(exitCode != 0){
			fileSystem.delete(tempDir, true);
			return exitCode;
		}

		Path result = new Path(outputDir, "result");		
		TestNaiveBayesDriver testBayes = new TestNaiveBayesDriver();
		testBayes.setConf(conf);
		exitCode = testBayes.run(new String[]{"-i", test.toString(), "-m", model.toString(), "-l", labels.toString(), "-ow", "-o", result.toString() });
		
		fileSystem.delete(tempDir, true);
		return exitCode;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ReutersClassifierRunner(), args));
	}

}
