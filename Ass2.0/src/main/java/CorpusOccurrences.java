import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CorpusOccurrences implements WritableComparable<CorpusOccurrences> {
    private int decade;
    private long numberOfOccurrences = 0;  //we are using with big data . is int enough?  //count of w1w2

    public CorpusOccurrences(int decade, long numberOfOccurrences){
        this.decade = decade;
        this.numberOfOccurrences = numberOfOccurrences;
    }
    @Override
    public int compareTo(CorpusOccurrences other) {
        return Math.abs(decade - other.getDecade());
    }

    public int getDecade(){
        return decade;
    }

    public long getNumberOfOccurrences(){
        return numberOfOccurrences;
    }

    public void setNumberOfOccurrences(long newNumberOfOccurrences){
        this.numberOfOccurrences = newNumberOfOccurrences;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(decade);
        dataOutput.writeLong(numberOfOccurrences);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        decade = dataInput.readInt();
        numberOfOccurrences = dataInput.readLong();
    }

    public String toString(){
        return decade+" "+numberOfOccurrences;
    }
}
