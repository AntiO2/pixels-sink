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
 
package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.sink.event.deserializer.TransactionJsonMessageDeserializer;

public class TransactionConfig
{
    public static final String DEFAULT_TRANSACTION_TOPIC_SUFFIX = "transaction";
    public static final String DEFAULT_TRANSACTION_TOPIC_VALUE_DESERIALIZER = TransactionJsonMessageDeserializer.class.getName();
    public static final String DEFAULT_TRANSACTION_TOPIC_GROUP_ID = "transaction_consumer";

    public static final String DEFAULT_TRANSACTION_TIME_OUT = "300";

    public static final String DEFAULT_TRANSACTION_BATCH_SIZE = "100";
    public static final String DEFAULT_TRANSACTION_MODE = "batch";
}
