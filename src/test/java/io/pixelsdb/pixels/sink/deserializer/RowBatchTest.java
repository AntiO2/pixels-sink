package io.pixelsdb.pixels.sink.deserializer;

import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class RowBatchTest {
    private final List<String> columnNames = new ArrayList<>();
    private final List<String> columnTypes = new ArrayList<>();

    @Test
    public void integerRowBatchTest() {
        columnNames.add("id");
        columnTypes.add("int");

        TypeDescription schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
        VectorizedRowBatch rowBatch = schema.createRowBatch(3, TypeDescription.Mode.CREATE_INT_VECTOR_FOR_INT);
        VectorizedRowBatch newRowBatch = VectorizedRowBatch.deserialize(rowBatch.serialize());

        return;
    }

    @Test
    public void varcharRowBatchTest() {
        columnNames.add("name");
        columnTypes.add("varchar(100)");

        TypeDescription schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
        VectorizedRowBatch rowBatch = schema.createRowBatch(3, TypeDescription.Mode.CREATE_INT_VECTOR_FOR_INT);
        BinaryColumnVector v = (BinaryColumnVector)rowBatch.cols[0];
        v.add("rr");
        v.add("zz");
        v.add("rr");
        VectorizedRowBatch newRowBatch = VectorizedRowBatch.deserialize(rowBatch.serialize());

        return;
    }
}
