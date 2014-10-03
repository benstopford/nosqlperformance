package com.benstopford.nosql.util;

public class Logger {
    static Logger logger = new Logger();
    public static Logger instance(){return logger;}

    public void info(Object o){
        System.out.println(o.toString());
    }

    public void info(String o, Object... args){
        System.out.println(String.format(o, args));
    }


}
