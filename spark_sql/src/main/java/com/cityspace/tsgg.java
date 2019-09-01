package com.cityspace;

/**
 * @author hq
 * @date 2019-08-29
 */
public class tsgg {
    public static void main(String[] args) {
        int[] is=new int[26];
        String str="fbmvxnbaobgfbsbzz";
        char[] chars = str.toCharArray();
        for (int i=0;i<chars.length;i++){
            char m=chars[i];
            System.out.println(m-'a');
            is[m-'a']++;
        }

//        for(int j=0;j<is.length;j++){
//            System.out.println((char)(97+j)+"  "+is[j]);
//        }

//        char c='a';
//        int n=(int)c;
//        System.out.println(n);




    }
}
