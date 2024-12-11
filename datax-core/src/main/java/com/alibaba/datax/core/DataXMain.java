package com.alibaba.datax.core;

import com.alibaba.datax.common.exception.DataXException;

import java.io.File;

public class DataXMain {

    public static void main(String[] args) {
        try {
            // 设置 DataX 运行目录和日志路径
            String s = "D:\\person-workspace\\DataX-all-in-java\\datax-core\\src\\main";
            String homePath = "D:\\person-workspace\\DataX-all-in-java\\target\\datax\\datax";
            System.setProperty("datax.home", homePath);
            // System.setProperty("log.file.path", System.getProperty("user.dir") + "/logs");
            System.out.println(System.getProperty("user.dir"));
            String[] strings = {"-job", "E:\\other-workspace\\datax-workspace\\demo.json", "-mode", "standalone", "-jobid", "-1"};
            // 调用核心引擎，执行 DataX 任务
            Engine.entry(strings);
        } catch (Throwable t) {
            System.err.println("DataX execution failed: " + t.getMessage());
            t.printStackTrace();
            System.exit(1);
        }
    }
}
