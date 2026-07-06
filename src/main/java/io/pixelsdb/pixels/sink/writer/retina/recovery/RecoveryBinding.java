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

package io.pixelsdb.pixels.sink.writer.retina.recovery;

import io.pixelsdb.pixels.sink.source.storage.StorageSourceOffset;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
public class RecoveryBinding implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final String dataSourceTxId;
    private final long pixelsTransId;
    private final long timestamp;
    private final long leaseStartMs;
    private final long leasePeriodMs;
    private final StorageSourceOffset beginOffset;
    @Setter
    private StorageSourceOffset lastSafeOffset;
    @Setter
    private RecoveryState state;

    public RecoveryBinding(
            String dataSourceTxId,
            long pixelsTransId,
            long timestamp,
            long leaseStartMs,
            long leasePeriodMs,
            StorageSourceOffset beginOffset,
            StorageSourceOffset lastSafeOffset,
            RecoveryState state)
    {
        this.dataSourceTxId = dataSourceTxId;
        this.pixelsTransId = pixelsTransId;
        this.timestamp = timestamp;
        this.leaseStartMs = leaseStartMs;
        this.leasePeriodMs = leasePeriodMs;
        this.beginOffset = beginOffset;
        this.lastSafeOffset = lastSafeOffset;
        this.state = state;
    }
}
