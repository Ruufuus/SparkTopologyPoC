package org.example;

import eu.europeana.enrichment.rest.client.report.ProcessedResult;
import eu.europeana.enrichment.rest.client.report.Type;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.stream.Collectors;


public class EnrichmentSpark {


    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(EnrichmentSpark.class);
    private static final Properties enrichmentProperties = new Properties();
    private static final String basePath = "/home/rafal/Pulpit/spark/";
    private static Enricher enricher;
    private static FileReader fileReader;
    private static SparkContext sparkContext;

    public static void main(String[] args) {
        if (args.length > 0) {
            init(args[0]);

            // Load our input data.
            JavaRDD<TaskData> filesToEnrich = loadTaskData();

            JavaRDD<TaskData> fileData = loadFileContent(filesToEnrich);

            JavaRDD<TaskData> enrichedFileData = enrichFileContent(fileData);

            saveTaskRecordStatuses(enrichedFileData);

            saveTaskReports(enrichedFileData);

            JavaRDD<Tuple2<String, String>> enrichedContent = enrichedFileData
                    .filter((taskData -> taskData.getResultFileContent() != null && !taskData.getProcessingStatus().equals(ProcessedResult.RecordStatus.STOP.toString())))
                    .map(taskData ->
                            new Tuple2<>(
                                    taskData.getTaskId(),
                                    taskData.getResultFileContent()
                            )
                    );
            enrichedContent.saveAsTextFile(basePath + "result");


            System.out.println("Input anything to end Spark process!");
            new Scanner(System.in).nextLine();
            sparkContext.cleaner();
        } else {
            LOGGER.error("Config file is not provided!, Please provide config file as program arguments");
        }
    }

    private static void saveTaskReports(JavaRDD<TaskData> enrichedFileData) {
        JavaRDD<Tuple2<String, List<String>>> enrichmentReports = enrichedFileData
                .map(
                        result -> new Tuple2<>(
                                result.getTaskId(),
                                result.getReportSet()
                                        .stream()
                                        .filter(report -> report.getMessageType() != Type.IGNORE)
                                        .map(report -> report.getMessageType() + ";" + report.getMessage() + ";" + report.getValue() + ";" + report.getMode() + ";" + report.getStackTrace())
                                        .collect(Collectors.toList())
                        )
                );
        enrichmentReports.saveAsTextFile(basePath + "task_reports");
    }

    private static void saveTaskRecordStatuses(JavaRDD<TaskData> enrichedFileData) {
        JavaRDD<Tuple2<String, String>> enrichmentStatuses = enrichedFileData
                .mapToPair(
                        result -> new Tuple2<>(
                                result.getProcessingStatus(),
                                new Tuple2<>(
                                        1,
                                        result.getTaskId()
                                )
                        )
                ).reduceByKey((a, b) -> new Tuple2<>(
                                a._1 + b._1,
                                a._2
                        )
                )
                .map(result -> new Tuple2<>(
                        result._2._2,
                        String.format("Level:%s, Count:%s", result._1, result._2._1)
                ));
        enrichmentStatuses.saveAsTextFile(basePath + "statuses");
    }

    private static JavaRDD<TaskData> enrichFileContent(JavaRDD<TaskData> fileData) {
        return fileData.map(fd -> {
            ProcessedResult<String> pr = enricher.enrich(fd.getFileContent());
            fd.setResultFileContent(pr.getProcessedRecord());
            fd.setReportSet(pr.getReport());
            fd.setProcessingStatus(pr.getRecordStatus().toString());
            return fd;
        });
    }

    private static JavaRDD<TaskData> loadFileContent(JavaRDD<TaskData> filesToEnrich) {
        return filesToEnrich.map(fd -> {
            fd.setFileContent(fileReader.readFileData(fd.getFileUrl()));
            return fd;
        });
    }

    private static JavaRDD<TaskData> loadTaskData() {
        return sparkContext.textFile(basePath + "input.txt", 3).toJavaRDD().map(taskData -> {
            String[] taskInfo = taskData.split(" ");
            return new TaskData(taskInfo[0], taskInfo[1], taskInfo[2]);
        });
    }

    @NotNull
    private static void init(String configFile) {
        try (FileInputStream fileInput = new FileInputStream(configFile)) {
            LOGGER.info("Config file provided!");
            enrichmentProperties.load(fileInput);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        enricher = new Enricher(
                enrichmentProperties.getProperty("DEREFERENCE_SERVICE_URL"),
                enrichmentProperties.getProperty("ENTITY_MANAGEMENT_URL"),
                enrichmentProperties.getProperty("ENTITY_API_URL"),
                enrichmentProperties.getProperty("ENTITY_API_KEY"));

        fileReader = new FileReader();

        // Disable logging
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        // Create a Java Spark Context.
        SparkConf sparkConf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.mb", "24")
                .setAppName("EnrichmentTopology")
                .setMaster("local[*]");
        sparkContext = new SparkContext(sparkConf);
    }
}
