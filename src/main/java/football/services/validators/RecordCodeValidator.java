package football.services.validators;

import football.configs.UserConfig;
import football.services.CustomUDF3;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@Service
public class RecordCodeValidator implements DataValidator,CustomUDF3 {

    @Autowired
    private UserConfig userConfig;

    @Override
    public Dataset validate(Dataset dataset) {
       // List<String> cols = Arrays.asList("to", "from");
       // Seq<String> argumentsSeq = scala.collection.JavaConversions.asScalaBuffer(
       //         Arrays.asList("to", "from")).seq();

        return dataset.withColumn("recordCodeInvalid",
                callUDF(UdfName(), col("to"), col("from"), col("code")));
    }

    @Override
    public String UdfName() {
        return "validateRecordCode";
    }

    @SneakyThrows
    @Override
    public String call(String player1, String player2, String code) {
        Boolean checkFail = player1!=null
                        && !player1.isEmpty()
                        && player2!=null
                        && !player2.isEmpty()
                        && userConfig.getSinglePlayerCodes().contains(code);

        return checkFail?"3":"";
    }
}
