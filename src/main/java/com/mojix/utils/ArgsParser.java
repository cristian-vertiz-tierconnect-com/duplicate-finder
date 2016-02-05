package com.mojix.utils;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Created by cvertiz on 2/2/16.
 */
public class ArgsParser {

    protected Options options;

    public ArgsParser()
    {
        options = new Options();
    }

    public void addOptions()
    {
        options.addOption( "d", "delete", false, "Delete duplicates without prompt" );

        options.addOption( "r", "restrict", false, "Restrict find duplicates in UDf query results" );

        options.addOption( "p", "parentThingType", true, "Thing type code of the parent" );

        options.addOption( "c", "childrenThingType", true, "Thing type code of the children" );

        options.addOption( "f", "file", true, "Csv file to search on" );

        options.addOption( "q", "query", true, "UDF query filter" );

    }

    public CommandLine parseOptions( String[] args )
    {
        CommandLineParser parser = new BasicParser();
        CommandLine line = null;
        try
        {
            line = parser.parse( options, args );
        }
        catch( ParseException exp )
        {
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "java -jar duplicate-finder.jar", options );
            System.exit( 1 );
        }

        if ( line.hasOption( "help" ) || line.getOptions().length == 0 )
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "java -jar duplicate-finder.jar -p <parentThingTypeCode> -c childrenThingTypeCode -f csvFilePath[-d] ", options );
            System.exit( 1 );
        }
        return line;

    }



}
