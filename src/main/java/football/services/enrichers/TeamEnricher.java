package football.services.enrichers;

import football.configs.UserConfig;
import football.services.CustomUDF;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Service
public class TeamEnricher implements DataEnricher, CustomUDF {

    @Autowired
    private UserConfig userConfig;

    @Override
    public Dataset addColumn(Dataset dataset) {
        dataset =  dataset.withColumn("teamFrom",
                callUDF(UdfName(),col("from")));//find country where the TO player from (if player not null)
        dataset =  dataset.withColumn("teamTo",
                callUDF(UdfName(),col("to")));//find country where the TO player from (if player not null)

        return dataset;
    }

    @Override
    public String UdfName() {
        return "findTeam";
    }

    @SneakyThrows
    @Override
    public String call(String player) {
        return userConfig.getTeams().get(player);
    }

}
