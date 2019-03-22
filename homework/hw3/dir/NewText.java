import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class NewText extends WritableComparator {

    public static final Text.Comparator TextComp = new Text.Comparator();
    public NewText() {
        super(Text.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return (-1) * TextComp.compare(b1,s1,l1,b2,s2,l2);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof Text && b instanceof Text) {
            return (-1) *(((Text) a).compareTo((Text) b));
        }

        return super.compare(a,b);
    }
}