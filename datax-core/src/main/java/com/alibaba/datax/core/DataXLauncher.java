package com.alibaba.datax.core;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

public class DataXLauncher {

    private static final String DATAX_VERSION = "DATAX-OPENSOURCE-3.0";
    private static final String DATAX_HOME = System.getProperty("datax.home", Paths.get("").toAbsolutePath().toString());
    private static final String DEFAULT_JVM = "-Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=" + DATAX_HOME + "/log";
    private static final String DEFAULT_PROPERTY_CONF = String.format("-Dfile.encoding=UTF-8 -Ddatax.home=%s -Dlogback.configurationFile=%s/conf/logback.xml", DATAX_HOME, DATAX_HOME);
    private static final String CLASS_PATH = String.format("%s/lib/*:.%s", DATAX_HOME, File.pathSeparator);

    public static void main(String[] args) {
        args = new String[]{"-m", "standalone", "-r", "mysqlreader", "-w", "mysqlwriter", "E:\\other-workspace\\dataX-workspace\\demo.json"};
        printCopyright();
        if (args.length == 0) {
            printHelp();
            System.exit(-1);
        }

        // Parse command-line arguments
        CommandLineOptions options = parseArguments(args);

        if (!options.isGenerateJobTemplate()) {
            generateJobConfigTemplate(options.getReader(), options.getWriter());
            return;
        }

        String jobResource = options.getJob();
        if (jobResource == null || jobResource.isEmpty()) {
            printHelp();
            System.exit(-1);
        }

        File jobFile = new File(jobResource);
        if (!jobFile.exists()) {
            System.err.println("Job file not found: " + jobResource);
            System.exit(-1);
        }

        // Build start command
        String startCommand = buildStartCommand(options, jobResource);
        System.out.println("Starting DataX: " + startCommand);
        // Execute command
        try {
            Process process = Runtime.getRuntime().exec(startCommand);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                process.destroy();
                System.out.println("DataX Process terminated.");
            }));
            process.waitFor();
            System.exit(process.exitValue());
        } catch (IOException | InterruptedException e) {
            System.err.println("Error during execution: " + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static String buildStartCommand(CommandLineOptions options, String jobResource) {
        String jvmParameters = options.getJvmParameters() != null ? options.getJvmParameters() : DEFAULT_JVM;
        if (options.isDebugMode()) {
            jvmParameters += " -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=9999";
        }

        String logLevel = options.getLogLevel() != null ? "-Dloglevel=" + options.getLogLevel() : "-Dloglevel=info";

        return String.format("java -server %s %s -classpath %s com.alibaba.datax.core.Engine -mode %s -jobid %s -job %s",
                jvmParameters, DEFAULT_PROPERTY_CONF, CLASS_PATH,
                options.getMode(), options.getJobId(), jobResource);
    }

    private static void generateJobConfigTemplate(String reader, String writer) {
        System.out.println("Generate job config template for:");
        System.out.println("Reader: " + reader);
        System.out.println("Writer: " + writer);
        System.out.println("Please refer to the documentation for configuration details.");
    }

    private static void printCopyright() {
        System.out.print("DataX (%s), From Alibaba!\n" +
                "       Copyright (C) 2010-2017, Alibaba Group. All Rights Reserved" +
                "DATAX_VERSION");
    }

    private static void printHelp() {
        System.out.println("Usage: java -jar datax.jar [options] job-url-or-path");
        System.out.println("Options:");
        System.out.println("  -j, --jvm <jvm parameters>   Set JVM parameters.");
        System.out.println("  -m, --mode <runtime mode>    Set runtime mode (standalone, local, distribute).");
        System.out.println("  -p, --params <parameters>    Set job parameters (e.g., -Dkey=value).");
        System.out.println("  -r, --reader <reader>        Specify reader for job template.");
        System.out.println("  -w, --writer <writer>        Specify writer for job template.");
        System.out.println("  -d, --debug                  Enable debug mode.");
        System.out.println("  --loglevel <level>           Set log level (debug, info, error).");
    }

    private static CommandLineOptions parseArguments(String[] args) {
        CommandLineOptions options = new CommandLineOptions();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-j":
                case "--jvm":
                    options.setJvmParameters(args[++i]);
                    break;
                case "-m":
                case "--mode":
                    options.setMode(args[++i]);
                    break;
                case "-p":
                case "--params":
                    options.setParams(args[++i]);
                    break;
                case "-r":
                case "--reader":
                    options.setReader(args[++i]);
                    break;
                case "-w":
                case "--writer":
                    options.setWriter(args[++i]);
                    break;
                case "-d":
                case "--debug":
                    options.setDebugMode(true);
                    break;
                case "--loglevel":
                    options.setLogLevel(args[++i]);
                    break;
                default:
                    options.setJob(args[i]);
            }
        }

        return options;
    }
}

class CommandLineOptions {
    private String jvmParameters;
    private String mode = "standalone";
    private String params;
    private String reader;
    private String writer;
    private String job;
    private String jobId = "-1";
    private String logLevel = "info";
    private boolean debugMode;

    public String getJvmParameters() {
        return jvmParameters;
    }

    public void setJvmParameters(String jvmParameters) {
        this.jvmParameters = jvmParameters;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public String getReader() {
        return reader;
    }

    public void setReader(String reader) {
        this.reader = reader;
    }

    public String getWriter() {
        return writer;
    }

    public void setWriter(String writer) {
        this.writer = writer;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public boolean isDebugMode() {
        return debugMode;
    }

    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
    }

    public boolean isGenerateJobTemplate() {
        return reader != null && writer != null;
    }
}
