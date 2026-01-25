/*
 * Copyright 2026 PixelsDB.
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

package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;

import java.util.concurrent.Semaphore;

public class InFlightControlManager
{

    private static volatile InFlightControlManager instance;
    private final Semaphore semaphore;

    private InFlightControlManager()
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        int MAX_IN_FLIGHT = config.getRetinaRpcLimit();
        this.semaphore = new Semaphore(MAX_IN_FLIGHT);
    }

    public static InFlightControlManager getInstance()
    {
        if (instance == null)
        {
            synchronized (InFlightControlManager.class)
            {
                if (instance == null)
                {
                    instance = new InFlightControlManager();
                }
            }
        }
        return instance;
    }

    public void acquire(int permits)
    {
        try
        {
            semaphore.acquire(permits);
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void release(int permits)
    {
        semaphore.release(permits);
    }
}
