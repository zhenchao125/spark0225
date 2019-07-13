package com.atguigu;

import java.util.Arrays;

/**
 * @Author lzc
 * @Date 2019-07-13 16:16
 */
public class REDemo {
    public static void main(String[] args) {
//        String s = "12603071531";
//        boolean result = s.matches("\\d{3}|\\d{5}");
//        boolean result = s.matches("1[3-9]\\d{9}");  // 验证手机号

//        String email = "abc@sohu.com.cn";
//        boolean result = email.matches("\\w{3,15}@\\S+\\.(com|cn|org|edu|info|com\\.cn)");
//        String s = "121112abc12a234";
//        String result = s.replaceAll("\\d+", "");\

        String ip = "...192..168.2.34.";
        String[] split = ip.split("\\.");
        System.out.println(Arrays.toString(split));

    }
}

/*
String:
    4个方法:
        matches   返回值是一个boolean, 用来判断字符串是否满足正则
        replaceAll  把满足正则的子字符串给替换掉
        replaceFirst  替换满足的第一个
        split   切割, 用正则去切割

语法:
    [abc] 表示一个字符, 要么a要么b要么c
    [a-z] 匹配所有小写字母
    [a-zA-Z] 匹配所有大小写字母
    [^ac]  非a和非c   如果把 ^ 放在 [] 中, 表示非
    \d   数字  ==== [0-9]   digital
    \D   非数字 ==== [^0-9]
    \w   单词字符 === [0-9a-zA-Z_]   word
    \W   非单词字符   [^0-9a-zA-Z_]
    .   表示任意字符串
    \. 匹配 .
    \s   sparce 空白字符  空格 \t \r \n
    \S  非空白

数量词:
    a? 0个或1个a  {0,1}
    a* 0个或多个a  {0,}
    a+ 1个或多个a  {1,}
    a{3,} 至少3个a
    a{3} 正好3个
    a{3,5} 至少3个a, 至多5个a  [3,5]

    a{3}|a{5} 3 个或 5 个a



正则表达式: RegularExpression

正则是工具, 处理文本的工具, 做文本的匹配用

字符串       贪官

正则         法律

------

主要涉及到两个类:
    Pattern   模式
    Matcher   匹配器

String:
    4个方法:
        matches   返回值是一个boolean, 用来判断字符串是否满足正则
        replaceAll  把满足正则的子字符串给替换掉
        replaceFirst  替换满足的第一个
        split   切割, 用正则去切割

正则的语法:

 */