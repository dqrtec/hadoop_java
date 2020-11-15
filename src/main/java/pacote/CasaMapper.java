package pacote;

/*
Fonte:
WHITE, T. Hadoop - The Definitive Guide. 4rd. ed. Sebastopol: O´Reilly, 2015.
*/

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CasaMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;
	
	@Override
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String campos = line.split(" ");
		Double valorCasa = Double.parseDouble( campos[ campos.length - 1 ] ); // campo do valor da casa
		
		context.write(new Text("valor"), new IntWritable(valorCasa));
	}
}