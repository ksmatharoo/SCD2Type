import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;

@AllArgsConstructor
public class SCDEngine {
    Dataset<Row> dataT1;
    Dataset<Row> dataT2;


    public Dataset<Row> process(String keyColumnName) {
        String prefix = "right";
        String rightKey = prefix + "_" + keyColumnName;
        Dataset<Row> updated = dataT1.join(dataT2.as(prefix), dataT1.col(keyColumnName).
                        equalTo(dataT2.col(keyColumnName))
                , "inner").select("right.*");

        //updated.show();

        Dataset<Row> ds1 = renameColumn(dataT2, prefix);
        Dataset<Row> left = dataT1.alias("left").join(ds1, dataT1.col(keyColumnName).
                        equalTo(ds1.col(rightKey))
                , "left").where(rightKey + " is null").select("left.*");

        //left.show();

        Dataset<Row> ds2 = renameColumn(dataT1, prefix);
        Dataset<Row> right = dataT2.alias("left").join(ds2, dataT2.col(keyColumnName).
                        equalTo(ds2.col(rightKey))
                , "left").where(rightKey + " is null").select("left.*");

        //right.show();

        return updated.union(left).union(right);
    }

    public Dataset<Row> renameColumn(Dataset<Row> ds, String prefix) {
        String[] objects = Arrays.stream(ds.columns()).map(f -> prefix + "_" + f).toArray(String[]::new);
        return ds.toDF(objects);
    }

}
