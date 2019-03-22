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


public class RelativeFreq1
{
      public static class PairsOccurrenceMapper extends Mapper<LongWritable, Text, WordPair1, IntWritable>
      {
          private WordPair1 wordPair = new WordPair1();
          private IntWritable ONE = new IntWritable(1);
          //private Text outputKey = new Text();
          //@Override
          protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
          {
              int neighbors = context.getConfiguration().getInt("neighbors", 2); // get the parameter from the driver. "2" is the default value in case "neighbors" is not set
                                                                                 // example: Configuration conf = new Configuration();
                                                                                 //          conf.setInt("neighbors", 2);
                                                                                 //          Job job = new Job(conf, "word pairs count");
              String[] tokens = value.toString().split("\\s+");
              if (tokens.length > 1)
              {
                for (int i = 0; i < tokens.length; i++)
                {
                    wordPair.setWord(tokens[i]);

                   int start = (i - neighbors < 0) ? 0 : i - neighbors;
                   int end = (i + neighbors > tokens.length - 1) ? tokens.length - 1 : i + neighbors;
                   for (int j = start; j <= end; j++)
                   {
                        if (j == i) continue;
                         wordPair.setNeighbor(tokens[j]);
                         context.write(wordPair, ONE);
                   }
                }
            }
        }
      }



      public static class PairsReducer extends Reducer<WordPair1,IntWritable,WordPair1,IntWritable>
      {
        private IntWritable totalCount = new IntWritable();
        @Override
        protected void reduce(WordPair1 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int count = 0;
            for (IntWritable value : values)
            {
                 count += value.get();
            }
            totalCount.set(count);
            context.write(key,totalCount);
        }
      }



  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word occurence");
    job.setJarByClass(RelativeFreq1.class);
    job.setMapperClass(PairsOccurrenceMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(PairsReducer.class);
    job.setOutputKeyClass(WordPair1.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
