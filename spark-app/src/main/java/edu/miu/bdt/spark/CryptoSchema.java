package edu.miu.bdt.spark;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public final class CryptoSchema {
    private CryptoSchema() {
    }

    public static StructType schema() {
        return new StructType()
                .add("symbol", DataTypes.StringType, false)
                .add("price", DataTypes.DoubleType, false)
                .add("timestamp", DataTypes.TimestampType, false);
    }
}
