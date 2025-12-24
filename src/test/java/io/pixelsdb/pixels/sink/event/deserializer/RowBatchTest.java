/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.event.deserializer;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class RowBatchTest
{
    private final List<String> columnNames = new ArrayList<>();
    private final List<String> columnTypes = new ArrayList<>();

    @Test
    public void integerRowBatchTest()
    {
        columnNames.add("id");
        columnTypes.add("int");

        TypeDescription schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
        VectorizedRowBatch rowBatch = schema.createRowBatch(3, TypeDescription.Mode.CREATE_INT_VECTOR_FOR_INT);
        VectorizedRowBatch newRowBatch = VectorizedRowBatch.deserialize(rowBatch.serialize());

    }

    @Test
    public void varcharRowBatchTest()
    {
        columnNames.add("name");
        columnTypes.add("varchar(100)");

        TypeDescription schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
        VectorizedRowBatch rowBatch = schema.createRowBatch(3, TypeDescription.Mode.CREATE_INT_VECTOR_FOR_INT);
        BinaryColumnVector v = (BinaryColumnVector) rowBatch.cols[0];
        v.add("rr");
        v.add("zz");
        v.add("rr");
        VectorizedRowBatch newRowBatch = VectorizedRowBatch.deserialize(rowBatch.serialize());

    }
}
