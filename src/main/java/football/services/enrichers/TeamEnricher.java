package football.services.enrichers;

import football.configs.UserConfig;
import lombok.SneakyThrows;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Service
public class TeamEnricher implements DataEnricher, CustomUDF {

    @Autowired
    private UserConfig userConfig;

    @Override
    public Dataset addColumn(Dataset dataset) {
        dataset =  dataset.withColumn("teamFrom",
                callUDF(UDFname(),col("from")));//find country where the TO player from (if player not null)
        dataset =  dataset.withColumn("teamTo",
                callUDF(UDFname(),col("to")));//find country where the TO player from (if player not null)

        return dataset;
    }

    @Override
    public String UDFname() {
        return "findTeam";
    }

    @SneakyThrows
    @Override
    public String call(String player) {
        return userConfig.getTeams().get(player);
    }

}
