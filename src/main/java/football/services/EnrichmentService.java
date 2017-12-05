package football.services;

import football.services.enrichers.DataEnricher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EnrichmentService {

    @Autowired
    private DatasetCreator datasetCreator;

    @Autowired
    private List<DataEnricher> dataEnrichers;

    public void enrich(){

        Dataset dataset = datasetCreator.createDataset();
        for (DataEnricher dataEnricher : dataEnrichers) {
            dataset = dataEnricher.addColumn(dataset);
        }
        dataset.show();
        //dataset.coalesce(1).write.option("header", "true").csv("data/football/results/enrichment_result.csv");
        /*dataset.write()
                .mode(SaveMode.Overwrite)
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("data/football/results/enrichment_result.txt");*/
    }
}