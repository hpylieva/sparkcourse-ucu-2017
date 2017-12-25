package football.services;

import org.apache.spark.sql.Dataset;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.Arrays;

import static football.constants.ConstProfiles.DEV;
import static org.apache.spark.sql.functions.col;

@Profile(DEV)
@Service
public class ValidationResultPrinter {

    public void showValidationResult(Dataset dataset, String[] initColNames){
        System.out.println("Data with INVALID columns:");
        dataset.filter(col("joinedValidation").notEqual(""))
                .drop("joinedValidation")
                .show();

        System.out.println("Data with VALID columns:");
        dataset.filter(col("joinedValidation").equalTo(""))
                .select( initColNames[0], Arrays.copyOfRange(initColNames, 1, initColNames.length))
                .show();
    }
}
