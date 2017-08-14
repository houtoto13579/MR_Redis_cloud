import java.io.*;
class Simple{  
    public static void main(String args[]){  
    try (Stream<String> lines = Files.lines(Paths.get("part-r-00000"))) {
    	line32 = lines.skip(31).findFirst().get();
	}
     System.out.println(line32);  
    }  
}  