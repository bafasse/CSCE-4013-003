import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class RelativeFreq2
{
      public  static class PairsRelativeOccurrenceMapper extends Mapper<LongWritable, Text, WordPair2, IntWritable> {

          private WordPair2 wordPair = new WordPair2();
          private IntWritable ONE = new IntWritable(1);
          private IntWritable count = new IntWritable();

          @Override
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
                  tokens[i] = tokens[i].replaceAll("\\W+","");
                  if(tokens[i].equals("")) {
                    continue;
                  }
                    wordPair.setWord(tokens[i]);

                   int start = (i - neighbors < 0) ? 0 : i - neighbors;
                   int end = (i + neighbors > tokens.length - 1) ? tokens.length - 1 : i + neighbors;
                   for (int j = start; j <= end; j++)
                   {
                        if (j == i) continue;
                         wordPair.setNeighbor(tokens[j]);
                         context.write(wordPair, ONE);
                   }
                   wordPair.setNeighbor("*");
                   count.set(end - start);
                   context.write(wordPair, count);
                }
            }
        }
      }



      public static class PairsRelativeOccurrenceReducer extends Reducer<WordPair2,IntWritable,WordPair2,DoubleWritable>
      {
        private DoubleWritable totalCount = new DoubleWritable();
        private DoubleWritable relCount = new DoubleWritable();
        private Text curWord = new Text("NOT_SET");
        private Text flag = new Text("*");
        
        @Override
        protected void reduce(WordPair2 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {

          if(key.getNeighbor().equals(flag)){
            if(key.getWord().equals(curWord)){
              totalCount.set(totalCount.get() + getTotalCount(values));
            }

            else {
              curWord.set(key.getWord());
              totalCount.set(0);
              totalCount.set(getTotalCount(values));
            }
          }

          else {

            int count = getTotalCount(values);
            relCount.set((double) count / totalCount.get());
            context.write(key, relCount);
          }
        
        }

        private int getTotalCount(Iterable<IntWritable> values) {

          int count = 0;
          for (IntWritable value : values)
          {
               count += value.get();
          }
          return count;
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
    job.setJarByClass(RelativeFreq2.class);
    job.setMapperClass(PairsRelativeOccurrenceMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(PairsRelativeOccurrenceReducer.class);
    job.setOutputKeyClass(WordPair2.class);
    job.setNumReduceTasks(3);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
