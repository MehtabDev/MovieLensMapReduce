package com.movie.kp2;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.movie.common.JoinWritable;

public class PreProcessReducer extends Reducer<Text, JoinWritable, Text, Text> {

	private List<Text> movieList = new ArrayList<Text>();
	private List<Text> ratingList = new ArrayList<Text>();

	@Override
	public void reduce(Text key, Iterable<JoinWritable> values, Context context)
			throws IOException, InterruptedException {
		movieList.clear();
		ratingList.clear();
		for (JoinWritable mow : values) {
			if (mow.getmFileName().toString().equals("movies.dat")) {
				movieList.add(new Text(mow.getmTitle()));
			} else if (mow.getmFileName().toString().equals("ratings.dat")) {
				ratingList.add(new Text(mow.getmTitle()));
			}
		}
		joinData(context);
	}

	private void joinData(Context context) throws IOException, InterruptedException {
		double sum = 0;
		if (!movieList.isEmpty() && !ratingList.isEmpty()) {
			for (Text moviesData : movieList) {
				for (Text ratingData : ratingList) {
					sum += Double.parseDouble(ratingData.toString());
				}
				if (ratingList.size() > 40) {
					double average = sum / ratingList.size();
					DecimalFormat df = new DecimalFormat("#.00"); // formatting the value for 2 digit
					context.write(moviesData, new Text(String.valueOf(df.format(average))));
				}

			}
		}
	}

}
