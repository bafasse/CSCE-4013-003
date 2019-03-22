import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

class WordPairMap extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException,InterruptedException
    {
        /*Get the name of the file using context.getInputSplit()method*/
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String line=value.toString();
        //Split the line in words
        String words[]=line.split(" ");
        for(String s:words){
            //for each word emit word as key and file name as value
            context.write(new Text(s), new Text(fileName));
        }
    }
}

class WordPairReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
        /*Declare the Hash Map to store File name as key to compute and store number of times the filename is occurred for as value*/
        HashMap m=new HashMap();
        int count=0;
        for(Text t:values){
            String str=t.toString();
            /*Check if file name is present in the HashMap ,if File name is not present then add the Filename to the HashMap and increment the counter by one , This condition will be satisfied on first occurrence of that word*/
            if(m!=null &&m.get(str)!=null){
                count=(int)m.get(str);
                m.put(str, ++count);
            } else{
                /*Else part will execute if file name is already added then just increase the count for that file name which is stored as key in the hash map*/
                m.put(str, 1);
            }
        }
        /* Emit word and [file1→count of the word1 in file1 , file2→count of the word1 in file2 ………] as output*/
        context.write(key, new Text(m.toString()));
    }
}