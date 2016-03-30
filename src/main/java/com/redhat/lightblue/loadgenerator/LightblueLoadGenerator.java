package com.redhat.lightblue.loadgenerator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.client.http.LightblueHttpClient;

/**
 * https://commons.apache.org/proper/commons-cli/usage.html
 * 
 * @author mpatercz
 *
 */
public class LightblueLoadGenerator {
    
    public static final Logger log = LoggerFactory.getLogger(LightblueLoadGenerator.class);
    
    public static void main(String[] args) throws ParseException {
        // create Options object
        Options options = new Options();
        
        Option lbClientOption = Option.builder("lc")
                .required(true)
                .desc("Configuration file for lightblue-client")
                .longOpt("lightblue-client")
                .hasArg()
                .argName("lightblue-client.properties")
                .build();
        
        Option queriesOption = Option.builder("q")
                .required(true)
                .desc("Property files with queries, merged in order provided")
                .longOpt("queries")
                .hasArgs()
                .argName("queries.properties")
                .build();              
        
        Option helpOption = Option.builder("h")
                .required(false)
                .desc("prints usage")
                .longOpt("help")                
                .build();

        Option runForeverOption = Option.builder()
                .required(false)
                .desc("Ignores the loop setting and runs forever")
                .longOpt("run-forever")
                .build();

        Option runForMinutesOption = Option.builder()
                .required(false)
                .desc("Ignores the loop setting and runs for specified number of minutes")
                .longOpt("run-for-minutes")
                .hasArg()
                .build();

        Option statsOption = Option.builder("s")
                .required(false)
                .desc("Enables stats. Note: stats consume resources.")
                .longOpt("stats")
                .build();

        Option statsDelayOption = Option.builder()
                .required(false)
                .desc("How often generate stats")
                .longOpt("stats-delay")
                .hasArg()
                .argName("seconds")
                .build();

        // add options
        options.addOption(lbClientOption);
        options.addOption(queriesOption);
        options.addOption(helpOption);
        options.addOption(runForeverOption);
        options.addOption(runForMinutesOption);
        options.addOption(statsOption);
        options.addOption(statsDelayOption);
        
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            
            if (cmd.hasOption('h')) {
                // automatically generate the help statement
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(LightblueLoadGenerator.class.getSimpleName(), options);
                System.exit(0);
            }
            
            String lbClientFilePath = cmd.getOptionValue("lc");
            String[] queriesFilePaths = cmd.getOptionValues("q");
            boolean runForver = cmd.hasOption("run-forever");
            boolean stats = cmd.hasOption("stats");
            Date runUntil = cmd.hasOption("run-for-minutes") ? new Date(new Date().getTime() + 1000*60*Integer.parseInt(cmd.getOptionValue("run-for-minutes"))) : null;
            int calculateStatsEveryMs = cmd.getOptionValue("stats-delay") == null ? 15000 : 1000*Integer.parseInt(cmd.getOptionValue("stats-delay"));

            if (stats)
                Stats.init(calculateStatsEveryMs);
            
            Properties p = new Properties();
            for (String queriesFilePath: queriesFilePaths) {
                try (InputStream is = Files.newInputStream(Paths.get(queriesFilePath))) {
                    p.load(is);
                } catch (IOException e) {
                    log.error("Can read " + queriesFilePath, e);
                    System.exit(1);
                }
            }

            LightblueHttpClient client = new LightblueHttpClient(lbClientFilePath);

            for(RQuery query: RQuery.fromProperties(p)) {
                for (int i = 0; i < query.getThreads(); i++) {
                    new Thread(new QueryRunner(query, client, runForver, runUntil, stats)).start();
                }
            }

        } catch (MissingOptionException e) {
            log.error(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(120, LightblueLoadGenerator.class.getSimpleName(), "", options, "See https://github.com/paterczm/lightblue-load-generator/");
            System.exit(1);
        }
    }
}
