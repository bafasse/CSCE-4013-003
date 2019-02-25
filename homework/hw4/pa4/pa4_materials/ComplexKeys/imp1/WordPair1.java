//package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.Text;
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
public class WordPair1 implements Writable,WritableComparable<WordPair1> {

    private Text word;
    private Text neighbor;

    public WordPair1(Text word, Text neighbor) {
        this.word = word;
        this.neighbor = neighbor;
    }

    public WordPair1(String word, String neighbor) {
        this(new Text(word),new Text(neighbor));
    }

    public WordPair1() {
        this.word = new Text();
        this.neighbor = new Text();
    }

    @Override
    public int compareTo(WordPair1 other) {                         // A compareTo B
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

    public static WordPair1 read(DataInput in) throws IOException {
        WordPair1 wordPair = new WordPair1();
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

        WordPair1 WordPair1 = (WordPair1) o;

        if (neighbor != null ? !neighbor.equals(WordPair1.neighbor) : WordPair1.neighbor != null) return false;
        if (word != null ? !word.equals(WordPair1.word) : WordPair1.word != null) return false;

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
