//package bbejeck.mapred.coocurrance;

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

// import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * User: Bill Bejeck
 * Date: 11/24/12
 * Time: 12:55 AM
 */
public class WordPair implements Writable,WritableComparable<WordPair> {

    private Text word;
    private Text neighbor;

    public WordPair(Text word, Text neighbor) {
        this.word = word;
        this.neighbor = neighbor;
    }

    public WordPair(String word, String neighbor) {
        this(new Text(word),new Text(neighbor));
    }

    public WordPair() {
        this.word = new Text();
        this.neighbor = new Text();
    }

    @Override
    public int compareTo(WordPair other) {                         // A compareTo B
        int returnVal = this.word.compareTo(other.getWord());      // return -1: A < B
        if(returnVal != 0){                                        // return 0: A = B
            return returnVal;                                      // return 1: A > B
        }
        if(this.neighbor.toString().equals("*")){
            return -1;
        }else if(other.getNeighbor().toString().equals("*")){
            return 1;
        }
        return this.neighbor.compareTo(other.getNeighbor());
    }

    public static WordPair read(DataInput in) throws IOException {
        WordPair wordPair = new WordPair();
        wordPair.readFields(in);
        return wordPair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        neighbor.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        neighbor.readFields(in);
    }

    @Override
    public String toString() {
        return "{word=["+word+"]"+
               " neighbor=["+neighbor+"]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordPair WordPair = (WordPair) o;

        if (neighbor != null ? !neighbor.equals(WordPair.neighbor) : WordPair.neighbor != null) return false;
        if (word != null ? !word.equals(WordPair.word) : WordPair.word != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (word != null) ? word.hashCode() : 0;
        result = 163 * result + ((neighbor != null) ? neighbor.hashCode() : 0);
        return result;
    }

    public void setWord(String word){
        this.word.set(word);
    }
    public void setNeighbor(String neighbor){
        this.neighbor.set(neighbor);
    }

    public Text getWord() {
        return word;
    }

    public Text getNeighbor() {
        return neighbor;
    }


}
