## Covid Data Analysis Tool
This tool takes a large dataset of number covid cases and fatalities on a given day. The data is also tagged with the region of the cases. The implements the **thread pooling** tactic to search the dataset to find out if a day had more than 30 fatalities (pool size is set to 10). Additionally, it uses a concurrent hash map and a priority queue to generate the current statistics for the dataset i.e. Top 10 countries with the highest fatalities, Top 10 days with most fatalities, Top 10 most fatal days for individual countries. This tool can be used by organizations like the WHO to decide which country needs the most aid. 

### How to run 
`cd .\coviddataprocessor\`

`javac .\CovidDataWorker.java .\CovidDataProcessor.java`

`java CovidDataProcessor`
