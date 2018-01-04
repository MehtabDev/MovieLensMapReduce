package com.movie.kp2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.movie.common.JoinWritable;
import com.movie.common.MovieMapper;
import com.movie.common.RatingMapper;

/**
 * @author mehtab khan
 * @description KPI1Driver class to start the process to resolve KPI1.
 */
public class KPI2Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: Path <in-1> <in-2> <out-3> <out-4>");
			System.exit(2);
		}
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(KPI2Driver.class);
		// setting separater into title and its rating for further use.
		job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
		TextOutputFormat.setOutputPath(job1, new Path(args[2]));
		// declaring this because Mapper output has different one from Reducer
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(JoinWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setReducerClass(PreProcessReducer.class);
		// MultipleInputs class to handle the multiple input files with respective
		// Mappers
		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

//		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
		int code = job1.waitForCompletion(true) ? 0 : 1;

		if (code == 0) { // if first job get executed successfully

			Job job2 = Job.getInstance(conf);
			job2.setJarByClass(KPI2Driver.class);
			job2.setJobName("Top viewed movies");
			// setting separater into title and its rating
			job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
			FileInputFormat.addInputPath(job2, new Path(args[2]));
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			
			job2.setMapperClass(KPI2Mapper.class);
			job2.setReducerClass(KPI2Reducer.class);
			
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
			
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
}
