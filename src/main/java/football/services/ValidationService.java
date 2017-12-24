package football.services;

import football.services.validators.DataValidator;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

import static org.apache.commons.lang3.ArrayUtils.subarray;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;

@Service
public class ValidationService {


    @Autowired
    private List<DataValidator> dataValidators;

    public void validate(Dataset dataset){

        //dataset = dataset.withColumn("validationFailureCodes", lit(""));

        String[] initColNames = dataset.schema().fieldNames();

        for (DataValidator dataValidator : dataValidators) {
            dataset = dataValidator.validate(dataset);
        }

        String[] afterValidationColNames = dataset.schema().fieldNames();
        afterValidationColNames = subarray(afterValidationColNames,initColNames.length,afterValidationColNames.length);

        
       Dataset dsWithJoinedValidation= dataset.withColumn("joinedValidation",
                concat(col(afterValidationColNames[0]),
                        col(afterValidationColNames[1]),
                        col(afterValidationColNames[2])) );

       System.out.println("Data with INVALID columns:");
       dsWithJoinedValidation.filter(col("joinedValidation").notEqual(""))
               .drop("joinedValidation")
               .show();

       System.out.println("Data with VALID columns:");
       dsWithJoinedValidation.filter(col("joinedValidation").equalTo(""))
               .select( initColNames[0], Arrays.copyOfRange(initColNames, 1, initColNames.length))
               .show();

        //dataset.coalesce(1).write().option("header", "true").csv("data/football/results/validation_result.csv");
       // return dataset;
    }

}
