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


import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;


public class CommandLineConfig
{
    private String configPath;

    public CommandLineConfig(String[] args)
    {
        ArgumentParser parser = ArgumentParsers
                .newFor("Pixels-Sink")
                .build()
                .defaultHelp(true)
                .description("pixels-sink kafka consumer");

        parser.addArgument("-c", "--config")
                .dest("config")
                .required(false)
                .help("config path");

        try
        {
            Namespace res = parser.parseArgs(args);
            this.configPath = res.getString("config");

        } catch (ArgumentParserException e)
        {
            parser.handleError(e);
            System.exit(1);
        }
    }

    public String getConfigPath()
    {
        return configPath;
    }

}