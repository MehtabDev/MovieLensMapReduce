package com.movie.kp1;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author mehtab khan
 * @description KPI1Reducer class to process the data to get the top 10 viewed
 *              movies.
 */
public class KPI1Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<Double, Text> mRatingData = new TreeMap<Double, Text>();

	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,
			Reducer<NullWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {

		for (Text value : values) {
			String[] mData = value.toString().split("::");
			if (mData.length == 2) {
				mRatingData.put(Double.parseDouble(mData[1]), new Text(value));
				if (mRatingData.size() > 10) {
					mRatingData.remove(mRatingData.firstKey());
				}
			}
		}
		for (Text topRating : mRatingData.descendingMap().values()) {
			context.write(NullWritable.get(), topRating);
		}
	}

}
