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

package io.pixelsdb.pixels.sink.source.storage;

import io.pixelsdb.pixels.sink.provider.ProtoType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageSourceOffsetTest
{
    @Test
    void shouldOrderOffsetsByEpochThenFileThenByteOffset()
    {
        StorageSourceOffset epoch1 = new StorageSourceOffset(0, 10L, 1, ProtoType.ROW);
        StorageSourceOffset file0 = new StorageSourceOffset(0, 10L, 0, ProtoType.ROW);
        StorageSourceOffset file1 = new StorageSourceOffset(1, 5L, 0, ProtoType.TRANS);
        StorageSourceOffset laterByteOffset = new StorageSourceOffset(1, 20L, 0, ProtoType.ROW);

        List<StorageSourceOffset> offsets = new ArrayList<>(List.of(epoch1, laterByteOffset, file1, file0));
        offsets.sort(StorageSourceOffset::compareTo);

        assertEquals(file0, offsets.get(0));
        assertEquals(file1, offsets.get(1));
        assertEquals(laterByteOffset, offsets.get(2));
        assertEquals(epoch1, offsets.get(3));
    }

    @Test
    void shouldIgnoreRecordTypeWhenComparingSamePhysicalPosition()
    {
        StorageSourceOffset rowOffset = new StorageSourceOffset(2, 30L, 4, ProtoType.ROW);
        StorageSourceOffset transOffset = new StorageSourceOffset(2, 30L, 4, ProtoType.TRANS);

        assertEquals(0, rowOffset.compareTo(transOffset));
        assertEquals(0, transOffset.compareTo(rowOffset));
        assertTrue(rowOffset.equals(new StorageSourceOffset(2, 30L, 4, ProtoType.ROW)));
    }
}
