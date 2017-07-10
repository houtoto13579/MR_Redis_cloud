import java.util.*;


public class SeqNoSuffixOffset implements Comparator<SeqNoSuffixOffset>, Comparable<SeqNoSuffixOffset>{
  public long seqNo; 
  public String suffix; 
  public int offset; 

  public SeqNoSuffixOffset(){
    seqNo  = 0;
    suffix = null;
    offset = 0;
  }

  // Overriding the compareTo method
  public int compareTo(SeqNoSuffixOffset other){
    return (this.suffix).compareTo(other.suffix);
  }

  // Overriding the compare method to sort the age 
  public int compare(SeqNoSuffixOffset s0, SeqNoSuffixOffset s1){
    return s0.suffix.compareTo(s1.suffix);
  }

  public String toString(){
    StringBuilder buffer = new StringBuilder(this.suffix.length()+5);
    buffer.append(this.suffix);
    buffer.append(" ");
    buffer.append(this.offset);

    return buffer.toString();
  }
}

