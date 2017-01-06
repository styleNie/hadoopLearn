package practice;

/**
 * Created by Administrator on 2016/12/28.
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FilterReader;
import java.io.IOException;

public class test {
    public static void main(String[] args)  {
        try {
            BufferedReader in = new BufferedReader(new FileReader("E:\\hadoop\\data\\100million"));
            String str;
            while ((str = in.readLine()) != null) {
                System.out.println(str);
            }
            System.out.println(str);
        } catch (IOException e) {
            System.out.print(e);
        }
    }
}
