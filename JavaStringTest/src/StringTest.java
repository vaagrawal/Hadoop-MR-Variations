public class StringTest {
	public static void main(String args[]) {
	String names= "Vaibahv,Anurag,Hatim,Bhim";
	String id[] = names.split(",");
	String fi = id[1]+id[3];
	System.out.println(fi);
	}
}
