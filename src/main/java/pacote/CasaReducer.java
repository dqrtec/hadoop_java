package pacote;

/*
Fonte:
WHITE, T. Hadoop - The Definitive Guide. 4rd. ed. Sebastopol: O´Reilly, 2015.
*/

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Max CasaReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sumHouseValue = 0;
		for (IntWritable value : values) {
			maxValue = maxValue + value.get();
		}
		context.write (maxValue, new IntWritable(maxValue));
	}
}