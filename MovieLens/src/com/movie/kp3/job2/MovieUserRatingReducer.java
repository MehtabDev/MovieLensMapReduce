package com.movie.kp3.job2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.movie.common.JoinWritable;


public class MovieUserRatingReducer extends Reducer<Text, JoinWritable, Text, Text> {

	private ArrayList<Text> movieList = new ArrayList<Text>();
	private ArrayList<Text> userRatingList = new ArrayList<Text>();
	private Text outputValue = new Text();

	@Override
	public void reduce(Text key, Iterable<JoinWritable> values, Context context)
			throws IOException, InterruptedException {
		movieList.clear();
		userRatingList.clear();
		for (JoinWritable mow : values) {

			if (mow.getmFileName().toString().equals("movies.dat")) {
				movieList.add(new Text(mow.getmTitle()));
			} else if (mow.getmFileName().toString().contains("part")) {
				userRatingList.add(new Text(mow.getmTitle()));
			}

		}
		joinFiles(context);
	}

	private void joinFiles(Context context) throws IOException, InterruptedException {
		if (!movieList.isEmpty() && !userRatingList.isEmpty()) {
			
			for (Text moviesData : movieList) {
				
				String[] data = moviesData.toString().split("::");
				String[] genres = data[1].split("\\|");
				
				for (Text ratingData : userRatingList) {
					
					for (String genre : genres) {
						outputValue.set(genre);
						context.write(outputValue, ratingData);
					}
				}
			}

		}
	}

}
