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
public class wordpair1 implements Writable,WritableComparable<wordpair1> {

    private Text word;
    private Text neighbor;

    public wordpair1(Text word, Text neighbor) {
        this.word = word;
        this.neighbor = neighbor;
    }

    public wordpair1(String word, String neighbor) {
        this(new Text(word),new Text(neighbor));
    }

    public wordpair1() {
        this.word = new Text();
        this.neighbor = new Text();
    }

    @Override
    public int compareTo(wordpair1 other) {
        int returnVal = this.word.compareTo(other.getWord());
        if(returnVal != 0){
            return returnVal;
        }
        if(this.neighbor.toString().equals("*")){
            return -1;
        }else if(other.getNeighbor().toString().equals("*")){
            return 1;
        }
        return this.neighbor.compareTo(other.getNeighbor());
    }

    public static wordpair1 read(DataInput in) throws IOException {
        wordpair1 wordPair = new wordpair1();
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

        wordpair1 wordPair = (wordpair1) o;

        if (neighbor != null ? !neighbor.equals(wordPair.neighbor) : wordPair.neighbor != null) return false;
        if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = word != null ? word.hashCode() : 0;
        result = 163 * result + (neighbor != null ? neighbor.hashCode() : 0);
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