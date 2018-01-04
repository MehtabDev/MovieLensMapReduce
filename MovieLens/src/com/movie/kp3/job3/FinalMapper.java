package com.movie.kp3.job3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalMapper extends Mapper<Object, Text, Text, Text> {

	private Text mKey = new Text();
	private Text mOutputValue = new Text();

	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
		String data = values.toString(); // Genres AgeRange Occupation Rating

		String[] field = data.split("::");

		if (null != field && field.length == 4) {
			mKey.set(field[2] + "::" + field[1]); // storing occupation and age range as key
			mOutputValue.set(field[0] + "::" + field[3]); // storing genres and rating as output
			context.write(mKey, mOutputValue);
		}
	}

}
