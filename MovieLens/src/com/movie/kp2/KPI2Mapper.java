package com.movie.kp2;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KPI2Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private TreeMap<Double, Text> mMapperTree = new TreeMap<Double, Text>();

	@Override
	protected void map(LongWritable key, Text values, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		String[] mData = values.toString().split("::"); // Toy Story (1995)::2077
		Double mRating = null;
		if (mData != null && mData.length == 2) {
			mRating = Double.parseDouble(mData[1]);
			Text value = new Text(values);

			mMapperTree.put(mRating, value);

			if (mMapperTree.size() > 20) {
				// if tree contains more than 10, so here we need to remove top first one
				// because by default tree map sorted with key with ascending order
				mMapperTree.remove(mMapperTree.firstKey());
			}

		}

	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		// At last cleanup method, so we here it means map method completed and now we have everything into our TreeMap
		if (mMapperTree != null && mMapperTree.size() > 0 ) {
			for (Map.Entry<Double, Text> mTreeData : mMapperTree.entrySet()) {
				context.write(NullWritable.get(), mTreeData.getValue());
				
			}
		}
	}


}
