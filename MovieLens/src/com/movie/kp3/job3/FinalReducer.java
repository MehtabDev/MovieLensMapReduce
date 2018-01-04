package com.movie.kp3.job3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.movie.util.GenresRanking;

public class FinalReducer extends Reducer<Text, Text, Text, Text> {

	private Map<String, GenresRanking> map = new HashMap<String, GenresRanking>();
	private TreeMap<Double, String> finalMap = new TreeMap<Double, String>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		map.clear();
		finalMap.clear();

		for (Text text : values) {
			String[] value = text.toString().split("::"); // genres and rating
			String genre = value[0];
			double rating = Double.parseDouble(value[1]);
			GenresRanking ranking = map.get(genre);
			if (ranking != null) {
				ranking.setSum(ranking.getSum() + rating);
				ranking.setCount(ranking.getCount() + 1);
			} else {
				GenresRanking rankingNew = new GenresRanking();
				rankingNew.setSum(rating);
				rankingNew.setCount(1);
				map.put(genre, rankingNew);
			}
		}
		for (Map.Entry<String, GenresRanking> entry : map.entrySet()) {
			GenresRanking gr = entry.getValue();
			double average = gr.getSum() / gr.getCount();
			finalMap.put(average, entry.getKey());
		}
		StringBuilder stringBuilder = new StringBuilder();

		for (Map.Entry<Double, String> entry : finalMap.descendingMap().entrySet()) {
				
			stringBuilder.append(entry.getValue() + "|");

		}
		context.write(key, new Text(stringBuilder.toString().substring(0, stringBuilder.toString().lastIndexOf("|"))));
	}

}
