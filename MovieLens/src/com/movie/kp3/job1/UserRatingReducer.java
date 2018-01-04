package com.movie.kp3.job1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.movie.common.JoinWritable;

public class UserRatingReducer extends Reducer<Text, JoinWritable, Text, Text> {

	private ArrayList<Text> userList = new ArrayList<Text>();
	private ArrayList<Text> ratingList = new ArrayList<Text>();

	@Override
	public void reduce(Text key, Iterable<JoinWritable> values, Context context)
			throws IOException, InterruptedException {
		userList.clear();
		ratingList.clear();
		
		for (JoinWritable mow : values) {

			if (mow.getmFileName().toString().equals("users.dat")) {
				userList.add(new Text(mow.getmTitle()));
			} else if (mow.getmFileName().toString().equals("ratings.dat")) {
				ratingList.add(new Text(mow.getmTitle()));
			}

		}
		joinFiles(context);
	}

	private void joinFiles(Context context) throws IOException, InterruptedException {
		if (!userList.isEmpty() && !ratingList.isEmpty()) {
			for (Text usersData : userList) {
				for (Text ratingData : ratingList) {
					context.write(usersData, ratingData);
				}
			}
		}
	}

}
