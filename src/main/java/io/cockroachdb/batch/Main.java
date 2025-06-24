package io.cockroachdb.batch;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

import io.cockroachdb.batch.util.DurationUtils;
import io.cockroachdb.batch.util.Multiplier;
import io.cockroachdb.batch.workload.MetricsPrinter;
import io.cockroachdb.batch.workload.WorkloadManager;

public class Main {
    // Add all tasks here with unique IDs / aliases
    private static final Map<String, Task> AVAILABLE_TASKS = Map.of(
            "fake", new FakeTask(),
            "array-insert", new ArrayInsertTask(),
            "batch-insert", new InsertTask()
    );

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private final WorkloadManager workloadManager;

    private final Map<String, String> params;

    private Map<String, Task> matchingTasks = Map.of();

    public Main(ExecutorService executorService, Map<String, String> params) {
        this.workloadManager = new WorkloadManager(executorService);
        this.params = params;
    }

    public void prepare(Set<String> taskNames) {
        // Filter tasks
        matchingTasks = AVAILABLE_TASKS.entrySet()
                .stream().filter(e -> taskNames.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (matchingTasks.isEmpty()) {
            throw new IllegalStateException("No tasks matching names: " + String.join(",", taskNames));
        }

        // Initialize tasks
        if (matchingTasks.values().stream()
                .anyMatch(task -> task instanceof DataSourceAware)) {
            DataSource dataSource = new DataSourceConfig().createDataSource(params);

            matchingTasks.forEach((id, task) -> {
                if (task instanceof DataSourceAware) {
                    ((DataSourceAware) task).setDataSource(dataSource);
                }
                if (task instanceof ExecutorAware) {
                    ((ExecutorAware) task).setExecutorService(workloadManager.getExecutorService());
                }
            });
        }

        // Prepare tasks
        matchingTasks.forEach((id, task) -> task.prepareTask(params));
    }

    public void run() {
        final int batchSize = Integer.parseInt(params.getOrDefault("batch-size", "64"));

        final Duration runtimeDuration = DurationUtils.parseDuration(params.getOrDefault("duration", "60s"));
        final int concurrency = Integer.parseInt(params.getOrDefault("concurrency", "1"));

        final Duration warmupDuration = DurationUtils.parseDuration(params.getOrDefault("warmup", "0s"));
        final int permits = Multiplier.parseInt(params.getOrDefault("permits", "5k"));

        final Instant stopTime = Instant.now().plus(runtimeDuration);
        final Instant warmupTime = Instant.now().plus(warmupDuration);

        final RateLimiter rateLimiter = RateLimiter.create(permits, warmupDuration);

        final MetricsPrinter metricsPrinter = new MetricsPrinter(workloadManager);

        if (!Boolean.parseBoolean(params.getOrDefault("disable-metrics", "false"))) {
            metricsPrinter.scheduleWithPeriod(5, TimeUnit.SECONDS);
        }

        logger.info("Scheduling %d tasks to run for %s with concurrency level %d and warmup period of %s - let it rip!"
                .formatted(matchingTasks.size(), runtimeDuration, concurrency, warmupDuration));

        // Schedule tasks
        matchingTasks.forEach((id, task) -> IntStream.rangeClosed(1, concurrency)
                .forEach(value -> {
                    final String title = id + " #" + value;

                    logger.info("Scheduling '%s' to run for %s with warmup %s"
                            .formatted(title, runtimeDuration, warmupDuration));

                    workloadManager.submitWorkload(task,
                            batchSize,
                            x -> {
                                if (Instant.now().isBefore(warmupTime)) {
                                    rateLimiter.acquire();
                                }
                                return Instant.now().isBefore(stopTime);
                            },
                            title);
                }));

        logger.info("All tasks scheduled - pending completion");

        workloadManager.shutdownAndWait();

        metricsPrinter.printSummary(batchSize);

        logger.info("All done ¯\\_(ツ)_/¯");
    }

    public static void printUsageAndQuit(String message) {
        if (!"".equals(message)) {
            System.out.println("ERROR: " + message);
            System.out.println();
        }

        System.out.println("Usage: java -jar batch-demo.jar [options] <task, ...>");

        System.out.println();
        System.out.println("Database options include:");
        System.out.println("--url <url>                  Connection URL (jdbc:postgresql://localhost:26257/defaultdb)");
        System.out.println("--user <user>                Login user name (root)");
        System.out.println("--password <secret>          Login password");
        System.out.println("--trace                      Enable SQL trace log");

        System.out.println();
        System.out.println("Concurrency options include:");
        System.out.println("--pool-size <size>           Max connection pool size (500)");
        System.out.println("--concurrency <level>        Number of threads per task (1)");
        System.out.println("--concurrency-limit <level>  Enables fixed-sized platform threads if non-zero. "
                           + "Default is unbounded virtual threads (-1)");

        System.out.println();
        System.out.println("Workload options include:");
        System.out.println("--batch-size <number>        Task batch size (64)");
        System.out.println("--duration <time>            Execution duration (60s)");
        System.out.println("--warmup <time>              Warmup duration (0s)");
        System.out.println("--permits <number>           Peak requests/sec at end of warmup (5k)");

        System.out.println();
        System.out.println("Task options include:");
        System.out.println("--disable-metrics            Disable task performance metrics");
        System.out.println("--param <k=v>                Custom task parameter tuple (see tasks for specific params)");

        System.exit(1);
    }

    public static void main(String[] args) {
        Set<String> tasks = new HashSet<>();
        Map<String, String> params = new HashMap<>();
        int concurrencyLimit = 0;

        LinkedList<String> argsList = new LinkedList<>(Arrays.asList(args));

        while (!argsList.isEmpty()) {
            String arg = argsList.pop();
            if (arg.equals("--url")) {
                if (argsList.isEmpty()) {
                    printUsageAndQuit("Expected URL after: " + arg);
                } else {
                    params.put("url", argsList.pop());
                }
            } else if (arg.equals("--user")) {
                if (argsList.isEmpty()) {
                    printUsageAndQuit("Expected username after: " + arg);
                } else {
                    params.put("user", argsList.pop());
                }
            } else if (arg.equals("--password")) {
                if (argsList.isEmpty()) {
                    printUsageAndQuit("Expected password after: " + arg);
                } else {
                    params.put("password", argsList.pop());
                }
            } else if (arg.equals("--trace")) {
                params.put("trace", "true");
            } else if (arg.equals("--pool-size")) {
                if (argsList.isEmpty()) {
                    printUsageAndQuit("Expected value after: " + arg);
                } else {
                    params.put("pool-size", argsList.pop());
                }
            } else if (arg.equals("--batch-size")) {
                if (argsList.isEmpty()) {
                    printUsageAndQuit("Expected value after: " + arg);
                } else {
                    params.put("batch-size", argsList.pop());
                }
            } else if (arg.equals("--concurrency")) {
                if (argsList.isEmpty()) {
                    printUsageAndQuit("Expected value after: " + arg);
                } else {
                    params.put("concurrency", argsList.pop());
                }
            } else if (arg.equals("--duration")) {
                if (argsList.isEmpty()) {
                    printUsageAndQuit("Expected duration after: " + arg);
                } else {
                    params.put("duration", argsList.pop());
                }
            } else if (arg.equals("--param")) {
                if (argsList.isEmpty()) {
                    printUsageAndQuit("Expected k/v tuple after: " + arg);
                } else {
                    String[] t = argsList.pop().split("=");
                    params.put(t[0], t[1]);
                }
            } else if (arg.equals("--disable-metrics")) {
                params.put("disableMetrics", "true");
            } else if (arg.equals("--concurrency-limit")) {
                if (argsList.isEmpty()) {
                    printUsageAndQuit("Expected value after: " + arg);
                } else {
                    concurrencyLimit = Integer.parseInt(argsList.pop());
                }
            } else if (arg.equals("--help")) {
                printUsageAndQuit("");
            } else if (arg.startsWith("--")) {
                printUsageAndQuit("Unrecognized option: '" + arg + "'");
            } else {
                tasks.add(arg);
            }
        }

        if (tasks.isEmpty()) {
            printUsageAndQuit("No task name specified. Available tasks: "
                              + String.join(", ", AVAILABLE_TASKS.keySet()));
        }

        if (concurrencyLimit > 0) {
            logger.info("Using bounded platform threads with pool size: {}", concurrencyLimit);
        } else {
            logger.info("Using unbounded virtual threads");
        }

        if (!params.isEmpty()) {
            logger.info("Task parameters:");
            params.forEach((k, v) -> logger.info("\t%s = %s".formatted(k, v)));
        }

        try (ExecutorService executorService = concurrencyLimit > 0
                ? Executors.newFixedThreadPool(concurrencyLimit)
                : Executors.newVirtualThreadPerTaskExecutor()) {
            Main main = new Main(executorService, params);
            main.prepare(tasks);
            main.run();
        }
    }
}
