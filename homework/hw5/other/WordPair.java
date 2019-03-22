import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapred.FileSplit;


class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
		
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException,InterruptedException
	{
		String line = value.toString();
		StringTokenizer token = new StringTokenizer(line);

		while(token.hasMoreTokens()) {
			word.set(token.nextToken());
			context.write(word, one);
		}
		// /*Get the name of the file using context.getInputSplit()method*/
		// String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		// String line=value.toString();
		// //Split the line in words
		// String words[]=line.split(" ");
		// for(String s:words){
		// //for each word emit word as key and file name as value
		// 	context.write(new Text(s), new Text(fileName));
		// }
	}
}

class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable value :values) {
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
