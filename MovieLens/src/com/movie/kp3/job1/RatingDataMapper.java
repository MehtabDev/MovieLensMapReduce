package com.movie.kp3.job1;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.movie.common.JoinWritable;

public class RatingDataMapper extends Mapper<Object, Text, Text, JoinWritable> {

	private String filename;
	private Text userId = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		FileSplit fileSplit = null;

		InputSplit split = context.getInputSplit();
		Class<? extends InputSplit> splitClass = split.getClass();

		if (splitClass.equals(FileSplit.class)) {
			fileSplit = (FileSplit) split;
		} else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
			// begin reflection hackery...

			try {
				Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
				getInputSplitMethod.setAccessible(true);
				fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
			} catch (Exception e) {
				// wrap and re-throw error
				throw new IOException(e);
			}
			filename = fileSplit.getPath().getName();
			// end reflection hackery
		}
	}

	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
		String data = values.toString(); // UserID::MovieID::Rating::Timestamp
		String[] field = data.split("::");
		
		if (null != field && field.length == 4 && field[0].length() > 0) {
			userId.set(field[0]);
			context.write(userId, new JoinWritable(field[1] + "::" + field[2], filename));
		}
	}

}
