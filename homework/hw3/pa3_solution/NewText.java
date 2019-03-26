
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NewText implements Writable,WritableComparable<NewText> {

    private Text word;

    public NewText(Text word) {
        this.word = word;
    }

    public NewText(String word) {
        this(new Text(word));
    }

    public NewText() {
        this.word = new Text();
    }

    @Override
    public int compareTo(NewText other) {                         	// A compareTo B
        int returnVal = this.word.compareTo(other.getWord());      	// return -1: A < B                                           										// return 0: A = B
            return (-1 * returnVal);                                    // return 1: A > B
    }

    public static NewText read(DataInput in) throws IOException {
        NewText wordPair = new NewText();
        wordPair.readFields(in);
        return wordPair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
    }

    @Override
    public String toString() {
        return word.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NewText wordPair = (NewText) o;
        if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = (word != null) ? word.hashCode() : 0;
        return result;
    }

    public void setWord(String word){
        this.word.set(word);
    }
    
    public Text getWord() {
        return word;
    }

}
