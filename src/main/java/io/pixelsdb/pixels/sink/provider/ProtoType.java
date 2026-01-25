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

package io.pixelsdb.pixels.sink.provider;


/**
 * @package: io.pixelsdb.pixels.sink.provider
 * @className: ProtoType
 * @author: AntiO2
 * @date: 2025/10/5 12:56
 */
public enum ProtoType
{
    ROW(0),
    TRANS(1);

    private final int value;

    ProtoType(int value)
    {
        this.value = value;
    }

    public static ProtoType fromInt(int value)
    {
        for (ProtoType type : ProtoType.values())
        {
            if (type.value == value)
            {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown ProtoType value: " + value);
    }

    public int toInt()
    {
        return value;
    }
}
