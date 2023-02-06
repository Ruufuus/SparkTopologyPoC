package org.example;

import eu.europeana.enrichment.rest.client.report.ProcessedResult;
import eu.europeana.enrichment.rest.client.report.Type;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;
import scala.Tuple3;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.stream.Collectors;


public class EnrichmentSpark {


    private static final Logger LOGGER = LogManager.getRootLogger();
    private static final Properties enrichmentProperties = new Properties();
    private static String basePath;
    private static Enricher enricher;
    private static FileManager fileManager;
    private static SparkContext sparkContext;

    private static int throttlingLevel;

    public static void main(String[] args) {
        if (args.length == 3) {
            init(args);


            LongAccumulator successfulTaskNumberAccumulator = sparkContext.longAccumulator();
            LongAccumulator failedTaskNumberAccumulator = sparkContext.longAccumulator();
            // Load our input data.
            JavaRDD<TaskData> filesToEnrich = loadTaskData();

            JavaRDD<TaskData> fileData = loadFileContent(filesToEnrich);

            int defaultPartitionCount = fileData.getNumPartitions();

            JavaRDD<TaskData> throttledData = throttleData(fileData);

            JavaRDD<TaskData> enrichedThrottledFileData = enrichFileContent(throttledData);

            JavaRDD<TaskData> enrichedFileData = rethrottleData(defaultPartitionCount, enrichedThrottledFileData);

            enrichedFileData.persist(StorageLevel.MEMORY_ONLY());

            saveTaskRecordStatuses(enrichedFileData);

            saveTaskReports(enrichedFileData);

            saveResult(successfulTaskNumberAccumulator, enrichedFileData);

            saveFailures(failedTaskNumberAccumulator, enrichedFileData);

            saveDiagnosticData(successfulTaskNumberAccumulator, failedTaskNumberAccumulator);
            System.out.println("Input anything to end Spark process!");
            new Scanner(System.in).nextLine();
            sparkContext.cleaner();
        } else {
            LOGGER.error("Config file is not provided!, Please provide config file as program arguments");
        }
    }

    // Throttling this way is resource consuming and might end up in out of memory exception due to limited partition memory
    // Additionally spark might run copy of task if it notices that processing is slow.
    private static JavaRDD<TaskData> rethrottleData(int defaultPartitionCount, JavaRDD<TaskData> enrichedThrottledFileData) {
        JavaRDD<TaskData> rethrottledData = enrichedThrottledFileData.repartition(defaultPartitionCount);
        LOGGER.info(String.format("Re-throttling data to %s partitions", rethrottledData.getNumPartitions()));
        return rethrottledData;
    }


    private static JavaRDD<TaskData> throttleData(JavaRDD<TaskData> fileData) {
        if (throttlingLevel > fileData.getNumPartitions()) {
            LOGGER.info("Current partition number is lower than throttle level. Partition count will stay same");
            return fileData;
        } else {
            JavaRDD<TaskData> throttledData = fileData.coalesce(throttlingLevel);
            LOGGER.info(String.format("Throttling data to %s partitions", throttledData.getNumPartitions()));
            return throttledData;
        }
    }


    // Instead of map reduce by key I used accumulators there.
    // It is not safe because it is used inside map function witch might be rerun when:
    // - task is progressing slowly making supervisor run copy of the task
    // - spark fails inside map function
    // - if supervisor crashes
    // accumulators are advised to be only used inside spark foreach functions which are not bound to modify any RDDs.

    private static void saveDiagnosticData(LongAccumulator successfulTaskNumberAccumulator, LongAccumulator failedTaskNumberAccumulator) {
        try {
            LOGGER.info("Saving diagnostic data");
            fileManager.saveFileData(basePath + "diagnostic_data", String.format("Number of successfully ended tasks:\t%s%nNumber of failed tasks:\t%s%n",
                    successfulTaskNumberAccumulator.value().toString(), failedTaskNumberAccumulator.value().toString()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void saveFailures(LongAccumulator failedTaskNumberAccumulator, JavaRDD<TaskData> enrichedFileData) {
        JavaRDD<Tuple2<String, String>> failures = enrichedFileData
                .filter(TaskData::isFailed)
                .map(taskData -> {
                    LOGGER.info("Incrementing failed task counter");
                    failedTaskNumberAccumulator.add(1);
                    return new Tuple2<>(taskData.getTaskId(), taskData.getFileUrl());
                });
        failures.saveAsTextFile(basePath + "failures");
    }

    private static void saveResult(LongAccumulator successfulTaskNumberAccumulator, JavaRDD<TaskData> enrichedFileData) {
        JavaRDD<Tuple3<String, String, String>> enrichedContent = enrichedFileData
                // Checking flag in order to stop processing
                .filter(efd -> !efd.isFailed())
                .filter((taskData -> taskData.getResultFileContent() != null && !taskData.getProcessingStatus().equals(ProcessedResult.RecordStatus.STOP.toString())))
                .map(taskData -> {
                            LOGGER.info("Incrementing Successful task counter");
                            successfulTaskNumberAccumulator.add(1);
                            return new Tuple3<>(
                                    taskData.getTaskId(),
                                    taskData.getFileUrl(),
                                    taskData.getResultFileContent()
                            );
                        }
                );
        enrichedContent.saveAsTextFile(basePath + "result");
    }

    private static void saveTaskReports(JavaRDD<TaskData> enrichedFileData) {
        JavaRDD<Tuple3<String, String, List<String>>> enrichmentReports = enrichedFileData
                .map(
                        result -> new Tuple3<>(
                                result.getTaskId(),
                                result.getFileUrl(),
                                result.getReportSet()
                                        .stream()
                                        .filter(report -> report.getMessageType() != Type.IGNORE)
                                        .map(report -> report.getMessageType() + ";" + report.getMessage() + ";" + report.getValue() + ";" + report.getMode() + ";" + report.getStackTrace())
                                        .collect(Collectors.toList())
                        )
                );
        enrichmentReports.saveAsTextFile(basePath + "task_reports");
    }

    // Counting how many records ended up with status STOP and how many with CONTINUE
    // Similar way of counting might be used for counting any multiple partition wide data.
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
            try {
                ProcessedResult<String> pr = enricher.enrich(fd.getFileContent());
                fd.setResultFileContent(pr.getProcessedRecord());
                fd.setReportSet(pr.getReport());
                fd.setProcessingStatus(pr.getRecordStatus().toString());
                fd.setFailed(ProcessedResult.RecordStatus.STOP.equals(pr.getRecordStatus()));
                if (Math.random() >= 0.5) {
                    LOGGER.info("Exception thrown!");
                    throw new RuntimeException();
                }
                return fd;
                // Detection exception threw in previous RDDs will require some kind of flag that will show it
            } catch (Exception e) {
                LOGGER.error("Exception while Enriching/dereference", e);
                fd.setFailed(true);
                return fd;
            }
        });
    }

    private static JavaRDD<TaskData> loadFileContent(JavaRDD<TaskData> filesToEnrich) {
        return filesToEnrich.map(fd -> {
            fd.setFileContent(fileManager.readFileData(fd.getFileUrl()));
            return fd;
        });
    }

    private static JavaRDD<TaskData> loadTaskData() {
        return sparkContext.textFile(basePath + "input", 3).toJavaRDD().map(taskData -> {
            LOGGER.info(String.format("New task data:%s", taskData));
            String[] taskInfo = taskData.split(" ");
            return new TaskData(taskInfo[0], taskInfo[1], taskInfo[2]);
        });
    }

    @NotNull
    private static void init(String[] args) {


        try (FileInputStream fileInput = new FileInputStream(args[0])) {
            LOGGER.info("Config file provided!");
            enrichmentProperties.load(fileInput);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        basePath = args[1];

        throttlingLevel = Integer.valueOf(args[2]);

        enricher = new Enricher(
                enrichmentProperties.getProperty("DEREFERENCE_SERVICE_URL"),
                enrichmentProperties.getProperty("ENTITY_MANAGEMENT_URL"),
                enrichmentProperties.getProperty("ENTITY_API_URL"),
                enrichmentProperties.getProperty("ENTITY_API_KEY"));

        fileManager = new FileManager();

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
