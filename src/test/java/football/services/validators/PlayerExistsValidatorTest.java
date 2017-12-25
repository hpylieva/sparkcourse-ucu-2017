package football.services.validators;

import football.configs.MainConfig;
import football.configs.UserConfig;
import football.services.RowsCreator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.stream.Collectors;

import static football.constants.ConstProfiles.DEV;
import static org.apache.spark.sql.functions.col;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = MainConfig.class)
@ActiveProfiles(DEV)

public class PlayerExistsValidatorTest {

    @Autowired
    PlayerExistsValidator playerExistsValidator;

    @Autowired
    private RowsCreator rowsCreator;

    @Autowired
    private transient SQLContext sqlContext;

    @Autowired
    private UserConfig userConfig;

    @Test
    public void testIdentifyNonExistingFromPlayer() throws Exception {
        String data = "code=2;from=Vasya Petrov;to=Andriy Pyatov;eventTime=2:12;stadion=they;startTime=21:39;";
        JavaRDD<Row> rowRdd = (JavaRDD<Row>) rowsCreator.createRowFromLine(data);
        List<String> columnNames = userConfig.getColumnNames();

        StructField[] fields = new StructField[columnNames.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = DataTypes.createStructField(columnNames.get(i), DataTypes.StringType, true);
        }
        Dataset dataset = sqlContext.
                createDataFrame(rowRdd,
                        DataTypes.createStructType(fields));
        Dataset result = playerExistsValidator.validate(dataset);

        Assert.assertEquals(result.select(col("playerNotFoundTo"))
                .as(Encoders.STRING())
                .collectAsList()
                .stream().collect(Collectors.joining(",")), "1");
    }

    @Test
    public void shouldReturnIncorrectToPlayerCode() throws Exception {
        String data = "code=2;from=Andriy Pyatov;to=Vasya Petrov;eventTime=2:12;stadion=they;startTime=21:39;";
        JavaRDD<Row> rowRdd = (JavaRDD<Row>) rowsCreator.createRowFromLine(data);
        List<String> columnNames = userConfig.getColumnNames();

        StructField[] fields = new StructField[columnNames.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = DataTypes.createStructField(columnNames.get(i), DataTypes.StringType, true);
        }
        Dataset dataset = sqlContext.
                createDataFrame(rowRdd,
                        DataTypes.createStructType(fields));
        Dataset result = playerExistsValidator.validate(dataset);

        Assert.assertEquals(result.select(col("playerNotFoundFrom"))
                .as(Encoders.STRING())
                .collectAsList()
                .stream().collect(Collectors.joining(",")), "1");
    }

   /* @Test
    public void shouldReturnOneCodeForBothParticipant() throws Exception {

    }

    @Test
    public void shouldNotReturnOneCodeForBothParticipant() throws Exception {

    }*/
}