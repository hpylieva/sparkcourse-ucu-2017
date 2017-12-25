package football.services.validators;

import football.services.CustomUDF1;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.springframework.stereotype.Service;

import static org.apache.commons.lang3.ArrayUtils.subarray;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.callUDF;

@Service
public class ValidationResultAccumulator implements CustomUDF1 {

    public Dataset accumulateVAlidationResults(Dataset dataset, String[] initColNames){

        String[] afterValidationColNames = dataset.schema().fieldNames();
        afterValidationColNames = subarray(afterValidationColNames,initColNames.length,afterValidationColNames.length);

        dataset = dataset.withColumn("joinedValidation",
                concat(col(afterValidationColNames[0]),
                        col(afterValidationColNames[1]),
                        col(afterValidationColNames[2]),
                        col(afterValidationColNames[3])) )
                .withColumn("validationPassed", callUDF(UdfName(),col("joinedValidation")))
                .drop( "joinedValidation")
                .drop(col(afterValidationColNames[0]))
                .drop(col(afterValidationColNames[1]))
                .drop(col(afterValidationColNames[2]))
                .drop(col(afterValidationColNames[3]));

        ValidationResultPrinter validationResultPrinter = new ValidationResultPrinter();
        validationResultPrinter.showValidationResult(dataset, initColNames);

        return dataset;
    }

    public String UdfName() { return "validationAggrigator"; }

    @SneakyThrows
    @Override
    public String call(String testFails) {
        return (testFails.length()>0) ? "no" :  "yes";
    }

}
