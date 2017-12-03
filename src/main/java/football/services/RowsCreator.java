package football.services;

import football.configs.UserConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class RowsCreator implements Serializable{
    @Autowired
    private UserConfig userConfig;

    public Row createRowFromLine(String line) {
        Map<String, String> map = FieldMapCreator.getMap(line);
        List<String> columnValues = new ArrayList<>();
        for (String columnName : userConfig.getColumnNames()) {
            columnValues.add(map.get(columnName));
        }
        return RowFactory.create(columnValues.toArray());
    }



}
