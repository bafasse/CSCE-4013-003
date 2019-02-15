package bbejeck.mapred.coocurrance;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NewText implements Writable,WritableComparable<NewText> {

	private Text word;
	private Text neighbor;

	public NewText(Text word, Text neighbor) {
		this.word = word;
		this.neighbor = neighbor;
		
	}

	public NewText(String word, String neighbor) {
		this(new Text(word), new Text(neighbor));
	}
	
	public NewText() {
		this.word = new Text();
		this.neighbor = new Text();
	}
	
	@Override
	public int compareTo(NewText other) {
		int decider = this.word.compareTo(other.getWord());
		if(decider !=0) {
			return decider;
		}
		if (this.neighbor.toString() < other.getNeighbor().toString()) {
			return -1;
		}
		
		else if (this.neighbor.toString() > other.getNeighbor().toString()) {
			return 1;
		}
		return this.neighbor.compareTo(other.getNeighbor());
	}
	
	public static NewText read(DataInput in) throws IOException {
		NewText newText = new NewText();
		newText.readFields(in);
		return newText;
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
		return "{word=["+word+"]"+ " neighbor=["+neighbor+"]}";
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		
		NewText newText = (NewText) o;
		if (neighbor != null ? !neighbor.equals(newText.neighbor) : newText.neighbor != null) return false;
		if (word != null ? !word.equals(newText.word) : newText.word != null) return false;
		
		return true;
	}
	
	@Override
	public int hashCode() {
		int result = (word != null) ? word.hashCode() : 0;
		result = 163 * result + ((neighbor != null) ? neighbor.hashCode() : 0);
		return result;
	}
	
	public void setWord(String word) {
		this.word.set(word);
	}
	
	public void setNeighbor(String neighbor) {
		this.neighbor.set(neighbor);
	}
	
	public Text getWord() {
		return word;
	}
	
	public Text getNeighbor() {
		return neighbor;
	}
	
}