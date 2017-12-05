package football.services.validators;

import football.services.CustomUDF;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.springframework.stereotype.Service;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;


@Service
public class TimeValidator implements DataValidator, CustomUDF {

    @Override
    public Dataset validate(Dataset dataset) {
        return dataset.withColumn("timeIsInvalid", callUDF(UDFname(),col("eventTime")));
    }

    @Override
    public String UDFname() {
        return "validateTime";
    }

    // Let's suppose that maximum game duration is 90+15*2=120 mins. If overall time is higher - error.
    // If there are more than 60 seconds - error.
    @SneakyThrows
    @Override
    public String call(String time) {
        int[] parsedTime  = Arrays.stream(time.split(":"))
                .map(String::trim).mapToInt(Integer::parseInt).toArray();
        return (parsedTime[1] >60 || parsedTime[0]>120 ) ? "1":"0";
    }
}
