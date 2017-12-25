package football.services.validators;

import football.configs.UserConfig;
import football.services.CustomUDF1;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Service
public class PlayerExistsValidator implements DataValidator, CustomUDF1 {

    @Autowired
    private UserConfig userConfig;

    @Override
    public Dataset validate(Dataset dataset) {

        return dataset.withColumn("playerNotFoundFrom", callUDF(UdfName(),col("from")))
                        .withColumn("playerNotFoundTo", callUDF(UdfName(),col("to")));
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
