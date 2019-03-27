
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.google.common.io.Files;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetDecimalExample {
    private static int SCALE = 4;
    private static int PRECISION = 32;
    private static String myDecimalSchemaDesc = "{\"type\": \"fixed\", \"size\":16, \"logicalType\": \"decimal\", \"precision\": " + PRECISION + ", \"scale\": " + SCALE + ", \"name\":\"mydecimaltype1\"}";

    private static int toParquet(LocalDate date) {
        LocalDateTime epoch = LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.MIDNIGHT);
        return (int) Duration.between(epoch, LocalDateTime.of(date, LocalTime.MIDNIGHT)).toDays();
    }

    private static class ReScalingDecimalConversion extends Conversions.DecimalConversion {

        private final RoundingMode roundingMode;

        public ReScalingDecimalConversion(RoundingMode roundingMode){
            this.roundingMode=roundingMode;
        }

        @Override
        public ByteBuffer toBytes(BigDecimal value, Schema schema, LogicalType type) {
            return super.toBytes(value.setScale(((LogicalTypes.Decimal)type).getScale(), roundingMode),schema,type);
        }

        @Override
        public GenericFixed toFixed(BigDecimal value, Schema schema, LogicalType type) {
            return super.toFixed(value.setScale(((LogicalTypes.Decimal)type).getScale(),roundingMode),schema,type);
        }

    }

    public static void main(String[] args) {
        System.out.println("Start");

        String schema = "{\"namespace\": \"org.myorganization.mynamespace\"," //Not used in Parquet, can put anything
                + "\"type\": \"record\"," //Must be set as record
                + "\"name\": \"myrecordname\"," //Not used in Parquet, can put anything
                + "\"fields\": ["
                + " {\"name\": \"myInteger\", \"type\": \"int\"}," //Required field
                + " {\"name\": \"myString\",  \"type\": [\"string\", \"null\"]},"
                + " {\"name\": \"myDecimal\", \"type\": ["+myDecimalSchemaDesc+", \"null\"]},"
                + " {\"name\": \"myDate\", \"type\": [{\"type\": \"int\", \"logicalType\" : \"date\"}, \"null\"]}"
                + " ]}";

        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema avroSchema = parser.parse(schema);

        try {
            Configuration conf = new Configuration();
            //conf.set("fs.s3a.access.key", "ACCESSKEY");
            //conf.set("fs.s3a.secret.key", "SECRETKEY");
            //Below are some other helpful settings
            //conf.set("fs.s3a.endpoint", "s3.amazonaws.com");
            //conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            //conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()); // Not needed unless you reference the hadoop-hdfs library.
            //conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName()); // Uncomment if you get "No FileSystem for scheme: file" errors.

            Path path = new Path(Files.createTempDir().toPath().resolve("test.pq").toString());

            //Use path below to save to local file system instead
            //Path path = new Path("data.parquet");

            //so it automagically converts BigDecimals
            GenericData dm = new GenericData();
            dm.addLogicalTypeConversion(new ReScalingDecimalConversion(RoundingMode.UNNECESSARY /*we believe that we can save all decimals without losing precision*/));

            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
                    .withSchema(avroSchema)
                    .withCompressionCodec(CompressionCodecName.GZIP)
                    .withDataModel(dm)
                    .withConf(conf)
                    .withPageSize(4 * 1024 * 1024) //For compression
                    .withRowGroupSize(16 * 1024 * 1024) //For write buffering (Page size)
                    .build()) {

                BigDecimal b = new BigDecimal("-99.9999");
                //BigDecimal b=new BigDecimal("0.0128");
                while (b.compareTo(new BigDecimal("99.9999")) < 0) {

                    GenericData.Record record = new GenericData.Record(avroSchema);
                    record.put("myInteger", 1);
                    record.put("myString", "string value 1");


                    //We can finally write our decimal to our record
                    //strip, so we test changing-scale bigdecimals!
                    record.put("myDecimal", b.stripTrailingZeros());//toParquet(b)

                    //Get epoch value


                    //We can write number of days since epoch into the record
                    record.put("myDate", toParquet(LocalDate.now()));


                    //System.out.println("b="+b);
                    //We only have one record to write in our example
                    writer.write(record);
                    b = b.add(new BigDecimal("0.0001"));
                }
            }

            try (ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(path)
                    .withDataModel(dm)
                    .withConf(conf)
                    .build()
            ) {
                GenericData.Record record;
                while ((record = reader.read()) != null) {
                    System.out.println("record: " + record.get("myInteger") + ", " + record.get("myString") + ", " + ((BigDecimal)record.get("myDecimal")).toPlainString() + ", " + LocalDate.ofEpochDay((Integer) record.get("myDate")));
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
    }
}