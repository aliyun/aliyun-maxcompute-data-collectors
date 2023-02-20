package org.apache.flink.odps.output.writer.upsert;

import com.aliyun.odps.Column;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.data.ArrayRecord;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.output.writer.file.RowBlockWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

public class RowUpsertWriter extends RowBlockWriter<Row> implements UpsertWriter<Row> {

    public RowUpsertWriter(Column[] cols,
                           FileWriter<ArrayRecord> fileWriter,
                           OdpsWriteOptions options) {
        super(cols, fileWriter, options);
        this.odpsRecord = fileWriter.newElement();
    }

    @Override
    public void upsert(Row row) throws IOException {
        reset();
        buildOdpsRecord(odpsRecord, row);
        this.fileWriter.upsert(odpsRecord);
    }

    @Override
    public void delete(Row rec) throws IOException {
        reset();
        buildOdpsRecord(odpsRecord, rec);
        this.fileWriter.delete(odpsRecord);
    }

    private void reset() {
        for (int i = 0; i < odpsRecord.getColumnCount(); ++i) {
            odpsRecord.set(i, null);
        }
    }
}
