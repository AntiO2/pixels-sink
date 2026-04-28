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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface RecoveryStore extends Closeable
{
    boolean hasRecoveryState();

    void reset();

    Optional<RecoveryBinding> getBinding(String dataSourceTxId);

    List<RecoveryBinding> loadActiveBindings();

    boolean hasCommitMarker(String dataSourceTxId);

    void saveNewBinding(RecoveryBinding binding);

    void saveTimestampReplayIndex(TimestampReplayIndexEntry entry);

    Optional<TimestampReplayIndexEntry> findReplayIndexAtOrBefore(long pixelsTimestamp);

    void advanceLastSafeOffset(String dataSourceTxId, StorageSourceOffset offset);

    void markCommitting(String dataSourceTxId);

    void markCommitted(String dataSourceTxId);

    @Override
    void close() throws IOException;
}
