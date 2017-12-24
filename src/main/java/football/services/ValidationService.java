package football.services;

import football.services.validators.DataValidator;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.apache.commons.lang3.ArrayUtils.subarray;
import static org.apache.spark.sql.functions.*;

@Service
public class ValidationService implements CustomUDF1 {

    @Autowired
    private List<DataValidator> dataValidators;

    private ValidationResultPrinter validationResultPrinter;

/* Probably not the best organization of processing validation results,
    at least I shouldn't have hardcoded the number of validators, but due to limited time
    and educational purpose of this project I stayed with this*/

    public Dataset validate(Dataset dataset){

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


        validationResultPrinter.showValidationResult(dsWithJoinedValidation, initColNames);

       Dataset dsFinal = dsWithJoinedValidation
                           .withColumn("validationPassed", callUDF(UdfName(),col("joinedValidation")))
                           .drop( "joinedValidation")
                           .drop(afterValidationColNames[0])
                           .drop(afterValidationColNames[1])
                           .drop(afterValidationColNames[2]);

        return(dsFinal);
    }


    public String UdfName() { return "validationManager"; }

    @SneakyThrows
    @Override
    public String call(String testFails) {
        return (testFails.length()>0) ? "no" :  "yes";
    }

}
