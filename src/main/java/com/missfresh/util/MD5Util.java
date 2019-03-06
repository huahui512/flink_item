package com.missfresh.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author wangzhihua
 * @date 2019-03-04 18:31
 */
public class MD5Util {

    public static void main(String[] args) {
        System.out.println(encryption("ddddd"));
        System.out.println(encryption("ddddo1"));
        System.out.println(encryption("ddddo2"));
    }

    public static  String encryption(String OrderNo) {
        String result = OrderNo+"354039456123789"+"andriod";
        String re_md5 = new String();
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(result.getBytes());
            byte b[] = md.digest();

            int i;

            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }

            re_md5 = buf.toString();

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return re_md5.toUpperCase();
    }
}
