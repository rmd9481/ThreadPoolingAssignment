package coviddataprocessor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CovidDataProcessor {

    private static final int THREAD_POOL_SIZE = 10;
    private static final String FILE_PATH = "src/data/covid_data.csv";
    private final String data;
    private static final ConcurrentHashMap<String, Double> countryFatalities = new ConcurrentHashMap<>();

    public CovidDataProcessor(String data) {
        this.data = data;
    }

    private static void processCSVData(ExecutorService executor) {
        try (BufferedReader br = new BufferedReader(new FileReader(FILE_PATH))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                Runnable worker = new CovidDataWorker(line);
                executor.execute(worker);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static ExecutorService createThreadPool() {
        return Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    }

    private static void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        while (!executor.isTerminated()) { }
        System.out.println("All tasks are completed");
    }

    private static void printStatistics() {
        PriorityQueue<Map.Entry<String, Double>> topCountries = new PriorityQueue<>(
                (a, b) -> Double.compare(b.getValue(), a.getValue())
        );

        topCountries.addAll(countryFatalities.entrySet());

        System.out.println("\nTop 10 countries with the highest fatalities:");
        for (int i = 0; i < 10 && !topCountries.isEmpty(); i++) {
            Map.Entry<String, Double> entry = topCountries.poll();
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    public void processCovidData() {
        try {
            Thread.sleep(10);
            String[] values = data.split(",");
            if (values.length >= 8) {
                double fatalities = Double.parseDouble(values[7]);
                if (fatalities > 30) {
                    String country = values[2];
                    String date = values[5];
                    System.out.println("[" + Thread.currentThread().getName() + "] " + country + ", " + date + ", " + fatalities + " fatalities");
                    countryFatalities.merge(country, fatalities, Double::sum);
                }
            } else {
                System.out.println("Invalid data format: " + data);
            }
        } catch (InterruptedException | NumberFormatException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ExecutorService executor = createThreadPool();
        processCSVData(executor);
        shutdownExecutor(executor);
        printStatistics();
    }
}
