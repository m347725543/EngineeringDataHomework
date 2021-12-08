package util;

public class strTest {
    public static void main(String[] args) {
        String s = "aaaa,das dasda. 5     dasdsada*";
//        for (String c : s.split("\\W|[0-9]"))
//            System.out.println(c);
        for (String c : s.split("[^a-zA-Z]+"))
            System.out.println(c);

    }
}
