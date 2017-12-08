package football.services.enrichers;

import football.configs.UserConfig;
import football.services.CustomUDF1;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Service
public class eventDescriptionEnricher implements DataEnricher, CustomUDF1 {
    @Autowired
    private UserConfig userConfig;

    @Override
    public Dataset addColumn(Dataset dataset) {
        return dataset.withColumn("eventDescription",
                callUDF(UdfName(),col("code")));
    }

    @Override
    public String UdfName() {
        return "findEventDescription";
    }

    @SneakyThrows
    @Override
    public String call(String code) {
        return userConfig.getCodes().get(code);
    }
}
