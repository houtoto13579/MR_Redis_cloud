import java.io.*;

public class fileReader {
    public static void main(String args[]){
        String line = null;
        try{
        File file = new File("part-r-00000");
        line = getContents(file);       
        }catch (Exception e){
   
        }finally{
            System.out.println(line);
        }
    }


    public static String getContents(File aFile) throws IOException {
        try (LineNumberReader rdr = new LineNumberReader(new FileReader(aFile))) {
            StringBuilder sb1 = new StringBuilder();
            StringBuilder sb2 = new StringBuilder();
            for (String line = null; (line = rdr.readLine()) != null;) {
                System.out.println(rdr.getLineNumber());
                if (rdr.getLineNumber() == 100) {
                    sb1.append(line).append(File.pathSeparatorChar);
                }
            }
            //return new String[] { sb1.toString(), sb2.toString() };
            return sb1.toString();
        }
    }
}