package football.services;

import football.services.validators.DataValidator;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ValidationService {

    @Autowired
    private DatasetCreator datasetCreator;

    @Autowired
    private List<DataValidator> dataValidators;

    public void validate(){

        Dataset dataset = datasetCreator.createDataset();
        for (DataValidator dataValidator : dataValidators) {
            dataset = dataValidator.validate(dataset);
        }
        dataset.show();
        //dataset.coalesce(1).write().option("header", "true").csv("data/football/results/validation_result.csv");
    }

}
