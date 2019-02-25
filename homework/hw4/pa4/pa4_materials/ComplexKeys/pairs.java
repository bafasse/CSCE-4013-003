public class PairsOccurrenceMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
    private WordPair wordPair = new WordPair();
    private IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int neighbors = context.getConfiguration().getInt("neighbors", 2); // get the parameter from the driver. "2" is the default value in case "neighbors" is not set
                                                                           // example: Configuration conf = new Configuration();
                                                                           //          conf.setInt("neighbors", 2);
                                                                           //          Job job = new Job(conf, "word pairs count");
        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 1) {
          for (int i = 0; i < tokens.length; i++) {
              wordPair.setWord(tokens[i]);

             int start = (i - neighbors < 0) ? 0 : i - neighbors;
             int end = (i + neighbors > tokens.length - 1) ? tokens.length - 1 : i + neighbors;
             for (int j = start; j <= end; j++) {
                  if (j == i) continue;
                   wordPair.setNeighbor(tokens[j]);
                   context.write(wordPair, ONE);
             }
          }
      }
  }
}


public class PairsReducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
    private IntWritable totalCount = new IntWritable();
    @Override
    protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
             count += value.get();
        }
        totalCount.set(count);
        context.write(key,totalCount);
    }
}