package football.services;

import football.aspects.ShowDataSetInTheEnd;
import football.services.enrichers.DataEnricher;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EnrichmentService {

  //  @Autowired
   // private DatasetCreator datasetCreator;

    @Autowired
    private List<DataEnricher> dataEnrichers;

    @ShowDataSetInTheEnd
    public Dataset enrich(Dataset dataset){

        for (DataEnricher dataEnricher : dataEnrichers) {
            dataset = dataEnricher.addColumn(dataset);
        }
       // dataset.show();

    //    DataFrameWriter dfWriter = new DataFrameWriter(dataset);
      //  dfWriter.csv("data/football/results/enrichment_result.csv");
     //   dataset.coalesce(1).write.option("header", "true").csv("data/football/results/enrichment_result.csv");
     /*   dataset.write()
                .mode(SaveMode.Overwrite)
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                .save("enrichment_result.csv");*/
       // dataset.write().text("enrichment_result.csv");
        return dataset;
    }
}