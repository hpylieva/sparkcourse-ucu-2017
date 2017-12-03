package football.services;

import football.services.enrichers.DataEnricher;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EnrichmentService {
    @Autowired
    private DatasetCreator datasetCreator;

    @Autowired
    private List<DataEnricher> dataEnrichers;

    public void doWork(){
        Dataset dataset = datasetCreator.createDataset();
        for (DataEnricher dataEnricher : dataEnrichers) {
            dataset = dataEnricher.addColumns(dataset);
        }
        dataset.show();
//        dataFrame.save("path", SaveMode.Append);

    }
}