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
 
package io.pixelsdb.pixels.sink.writer;


public enum PixelsSinkMode
{
    CSV,
    RETINA,
    PROTO,
    FLINK,
    NONE;

    public static PixelsSinkMode fromValue(String value)
    {
        for (PixelsSinkMode mode : values())
        {
            if (mode.name().equalsIgnoreCase(value))
            {
                return mode;
            }
        }
        throw new RuntimeException(String.format("Can't convert %s to writer type", value));
    }
}
