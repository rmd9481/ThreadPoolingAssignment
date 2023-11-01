import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    private static final int POOL_SIZE = 10; // Number of threads in the pool
    private static final String FILE_PATH = "/Users/sushanthnayak/Downloads/train.csv";
   
    public static void main(String[] args) {
        // Creating a thread pool
        ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);

        // Reading the CSV file
        try (BufferedReader br = new BufferedReader(new FileReader(FILE_PATH))) {
            String line;
            // Reading the header line
            br.readLine();
            while ((line = br.readLine()) != null) {
                // Dividing the dataset and assigning to each thread
                Runnable worker = new CovidDataProcessor(line);
                executor.execute(worker); // Execute the task using a thread from the pool
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Shutdown the executor when all tasks are completed
        executor.shutdown();
        while (!executor.isTerminated()) {
            // Wait until all threads are finished
        }
        System.out.println("All tasks are completed");
    }

    // Hypothetical task: processing COVID data from a CSV file
    private static class CovidDataProcessor implements Runnable {
        private final String data;

        public CovidDataProcessor(String data) {
            this.data = data;
        }

        @Override
        public void run() {
            // Simulated time-consuming task of processing COVID data
            try {
                // Simulating a delay to make the task time-consuming
                Thread.sleep(10);
                // Simulating a 1-second delay

                String[] values = data.split(",");
                if (values.length >= 8 && !values[7].equals("Fatalities")) {
                    double fatalities = Double.parseDouble(values[7]);
                    if (fatalities > 10) {
                        System.out.println("Country with fatalities > 10: " + values[2] + " by Thread: " + Thread.currentThread().getName());
                    }
                } else {
                    // Handle the case where the data does not have enough columns or contains headers
                    System.out.println("Invalid data format: " + data);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (NumberFormatException e) {
                // Handle the case where the fatalities value is not a valid number
                e.printStackTrace();
            }
        }
    }
}
