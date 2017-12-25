package football.services;

import football.aspects.ShowDataSetInTheEnd;
import football.services.validators.DataValidator;
import football.services.validators.ValidationResultAccumulator;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ValidationService  {

    @Autowired
    private List<DataValidator> dataValidators;

/* Probably not the best organization of processing validation results,
    at least I shouldn't have hardcoded the number of validators, but due to limited time
    and educational purpose of this project I stayed with this*/

    @ShowDataSetInTheEnd
    public Dataset validate(Dataset dataset){

        String[] initColNames = dataset.schema().fieldNames();

        for (DataValidator dataValidator : dataValidators) {
            dataset = dataValidator.validate(dataset);
        }

        ValidationResultAccumulator validationResultAccumulator = new ValidationResultAccumulator();
        dataset= validationResultAccumulator.accumulateVAlidationResults(dataset, initColNames);


        return dataset;
    }



}
