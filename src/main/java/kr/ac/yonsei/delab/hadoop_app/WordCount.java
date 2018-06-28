package kr.ac.yonsei.delab.hadoop_app;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Scanner;

public class WordCount extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	}
 
	public int run(String[] args) throws Exception {
		Scanner scanner = new Scanner(System.in);
		System.out.print("Enter input files: ");
		String inputFile = scanner.nextLine();
		System.out.print("Enter output directory: ");
		String outputDir = scanner.nextLine();
		scanner.close();
		
		// Invalid I/O exception
		if (inputFile.length() == 0 || outputDir.length() == 0) {
			System.err.printf("Error: Invalid input file or output directory | Input file='%s', Output directory='%s'\n", getClass().getSimpleName(), inputFile, outputDir);
			return -1;
		}
	
		// Make job
		Job job = new Job();
		job.setJarByClass(WordCount.class);
		job.setJobName("WordCounter");
		
		// Set path
		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
	
		// Set output and Map&Reduce class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
	
		int returnValue = job.waitForCompletion(true) ? 0:1;
		
		// Print job result
		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");			
		}
		
		return returnValue;
	}
}