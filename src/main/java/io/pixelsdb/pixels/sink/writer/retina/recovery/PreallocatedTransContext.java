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

import io.pixelsdb.pixels.common.transaction.TransContext;

import java.io.Serializable;

public class PreallocatedTransContext implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final long pixelsTransId;
    private final long timestamp;
    private final long leaseStartMs;
    private final long leasePeriodMs;

    public PreallocatedTransContext(long pixelsTransId, long timestamp, long leaseStartMs, long leasePeriodMs)
    {
        this.pixelsTransId = pixelsTransId;
        this.timestamp = timestamp;
        this.leaseStartMs = leaseStartMs;
        this.leasePeriodMs = leasePeriodMs;
    }

    public long getPixelsTransId()
    {
        return pixelsTransId;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public long getLeaseStartMs()
    {
        return leaseStartMs;
    }

    public long getLeasePeriodMs()
    {
        return leasePeriodMs;
    }

    public static PreallocatedTransContext from(TransContext transContext)
    {
        return new PreallocatedTransContext(
                transContext.getTransId(),
                transContext.getTimestamp(),
                transContext.getLease().getStartMs(),
                transContext.getLease().getPeriodMs()
        );
    }

    public TransContext toTransContext()
    {
        return new TransContext(
                pixelsTransId,
                timestamp,
                leaseStartMs,
                leasePeriodMs,
                false
        );
    }
}
