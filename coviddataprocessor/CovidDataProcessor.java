import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CovidDataProcessor {

    private static final int THREAD_POOL_SIZE = 10;
    private static final String FILE_PATH = "../data/covid_data.csv";
    private final String data;
    private static final ConcurrentHashMap<String, Double> countryFatalities = new ConcurrentHashMap<>();
    private static final PriorityQueue<Map.Entry<String, Double>> topFatalitiesInOneDay = new PriorityQueue<>(
            (a, b) -> Double.compare(b.getValue(), a.getValue())
    );
    private static final ConcurrentHashMap<String, Map.Entry<String, Double>> topFatalDays = new ConcurrentHashMap<>();
    private static final PriorityQueue<Map.Entry<String, Double>> mostFatalDays = new PriorityQueue<>(
            (a, b) -> Double.compare(b.getValue(), a.getValue())
    );
    private static final ConcurrentHashMap<String, Map.Entry<String, Double>> mostFatalDayPerCountry = new ConcurrentHashMap<>();


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
        System.out.println("All tasks are completed!");
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

        System.out.println("\nTop 10 days with most fatalities:");
        for (int i = 0; i < 10 && !topFatalitiesInOneDay.isEmpty(); i++) {
            Map.Entry<String, Double> entry = topFatalitiesInOneDay.poll();
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        System.out.println("\nTop 10 most fatal days for individual countries:");
        mostFatalDays.addAll(mostFatalDayPerCountry.values());
        for (int i = 0; i < 10 && !mostFatalDays.isEmpty(); i++) {
            Map.Entry<String, Double> entry = mostFatalDays.poll();
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    private String[] parseCSV(String data) {
        List<String> items = new ArrayList<>();
        boolean notInsideQuote = true;
        StringBuilder value = new StringBuilder();
        for (char c : data.toCharArray()) {
            switch (c) {
                case ',':
                    if (notInsideQuote) {
                        items.add(value.toString());
                        value.setLength(0);
                    } else {
                        value.append(c);
                    }
                    break;
                case '"':
                    notInsideQuote = !notInsideQuote;
                    break;
                default:
                    value.append(c);
                    break;
            }
        }
        items.add(value.toString());
        return items.toArray(new String[0]);
    }

    public void processCovidData() {
        try {
            Thread.sleep(1);

            String[] values = parseCSV(data);

            if (values.length >= 8) {
                double fatalities = Double.parseDouble(values[7]);
                if (fatalities > 30) {
                    String country = values[2];
                    String date = values[5];
                    String key = country + " on " + date;

                    System.out.println("[" + Thread.currentThread().getName() + "] " + country + ", " + date + ", " + fatalities + " fatalities");
                    countryFatalities.merge(country, fatalities, Double::sum);

                    Map.Entry<String, Double> currentEntry = new AbstractMap.SimpleEntry<>(key, fatalities);
                    topFatalitiesInOneDay.offer(currentEntry);

                    topFatalDays.compute(key, (k, v) -> {
                        if (v == null || v.getValue() < fatalities) {
                            return currentEntry;
                        }
                        return v;
                    });

                    mostFatalDayPerCountry.compute(country, (k, v) -> {
                        if (v == null || v.getValue() < fatalities) {
                            return currentEntry;
                        }
                        return v;
                    });
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
