package com.movie.kp3.job3;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import com.movie.common.JoinWritable;
import com.movie.kp3.job1.RatingDataMapper;
import com.movie.kp3.job1.UserDataMapper;
import com.movie.kp3.job1.UserRatingReducer;
import com.movie.kp3.job2.MovieDataMapper;
import com.movie.kp3.job2.MovieUserRatingReducer;
import com.movie.kp3.job2.UserRatingMapper;
import com.movie.kp3.job3.FinalMapper;
import com.movie.kp3.job3.FinalReducer;

public class KPI3Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		int code2 = -1;
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 6) {
			System.err.println("Usage: Path <in-1> <in-2> <in-3> <out-4> <out-5> <out-6>");
			System.exit(2);
		}
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(KPI3Driver.class);

		job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
		TextOutputFormat.setOutputPath(job1, new Path(args[3])); // 2
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(JoinWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setReducerClass(UserRatingReducer.class);

		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, UserDataMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, RatingDataMapper.class);
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3])); // 2

		int code1 = job1.waitForCompletion(true) ? 0 : 1;

		if (code1 == 0) {

			Job job2 = Job.getInstance(conf);
			job2.setJarByClass(KPI3Driver.class);

			job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
			TextOutputFormat.setOutputPath(job2, new Path(args[4]));
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(JoinWritable.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			job2.setReducerClass(MovieUserRatingReducer.class);
// 3
			//2
			MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieDataMapper.class);
			MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, UserRatingMapper.class);
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));  // 4

			code2 = job2.waitForCompletion(true) ? 0 : 1;
		}
		if (code2 == 0) {
			Job job3 = Job.getInstance(conf);
			job3.setJarByClass(KPI3Driver.class);

//			job3.getConfiguration().set("mapreduce.output.textoutputformat.separator", "|");
			TextOutputFormat.setOutputPath(job3, new Path(args[5])); // 5

			job3.setMapperClass(FinalMapper.class);
			job3.setReducerClass(FinalReducer.class);

			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job3, new Path(args[4]));  // 4
			FileOutputFormat.setOutputPath(job3, new Path(otherArgs[5])); // 5 

			System.exit(job3.waitForCompletion(true) ? 0 : 1);
		}

	}

}
