package football.services.validators;

import football.configs.UserConfig;
import football.services.CustomUDF;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Service
public class PlayerExistsValidator implements DataValidator, CustomUDF {

    @Autowired
    private UserConfig userConfig;

    @Override
    public Dataset validate(Dataset dataset) {

        return dataset.withColumn("playerNotFound", callUDF(UdfName(),col("to")))
                      .withColumn("playerNotFound", callUDF(UdfName(),col("from")));
    }

    @Override
    public String UdfName() {
        return "validatePlayerExist";
    }

    @SneakyThrows
    @Override
    public String call(String player){
        return userConfig.getTeams().keySet().contains(player)?"":"1";
    }
}
