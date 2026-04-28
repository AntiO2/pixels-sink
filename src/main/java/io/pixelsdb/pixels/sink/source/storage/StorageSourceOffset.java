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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serial;
import java.io.Serializable;

@Getter
@ToString
@EqualsAndHashCode
public class StorageSourceOffset implements Comparable<StorageSourceOffset>, Serializable
{
    @Serial
    private static final long serialVersionUID = 1L;

    private final int fileId;
    private final long byteOffset;
    private final int epoch;
    private final ProtoType recordType;

    public StorageSourceOffset(int fileId, long byteOffset, int epoch, ProtoType recordType)
    {
        this.fileId = fileId;
        this.byteOffset = byteOffset;
        this.epoch = epoch;
        this.recordType = recordType;
    }

    @Override
    public int compareTo(StorageSourceOffset other)
    {
        int epochComparison = Integer.compare(this.epoch, other.epoch);
        if (epochComparison != 0)
        {
            return epochComparison;
        }
        int fileComparison = Integer.compare(this.fileId, other.fileId);
        if (fileComparison != 0)
        {
            return fileComparison;
        }
        return Long.compare(this.byteOffset, other.byteOffset);
    }
}
