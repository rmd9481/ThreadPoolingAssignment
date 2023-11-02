public class CovidDataWorker implements Runnable {

    private final CovidDataProcessor processor;

    public CovidDataWorker(String data) {
        this.processor = new CovidDataProcessor(data);
    }

    @Override
    public void run() {
        processor.processCovidData();
    }
}
