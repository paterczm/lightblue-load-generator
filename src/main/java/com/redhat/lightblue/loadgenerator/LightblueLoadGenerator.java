package com.redhat.lightblue.loadgenerator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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
                .desc("lightblue-client.properties file")
                .longOpt("lightblue-client")
                .hasArg()
                .build();
        
        Option queriesOption = Option.builder("q")
                .required(false)
                .desc("queries.properties file")
                .longOpt("queries")
                .hasArg()                
                .build();              
        
        Option helpOption = Option.builder("h")
                .required(false)
                .desc("prints usage")
                .longOpt("help")                
                .build(); 

        // add options
        options.addOption(lbClientOption);
        options.addOption(queriesOption);
        options.addOption(helpOption);
        
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
            String queriesFilePath = cmd.getOptionValue("q") != null ? cmd.getOptionValue("q") : "queries.properties";
            
            try (InputStream is = Files.newInputStream(Paths.get(queriesFilePath))) {
                Properties p = new Properties();
                p.load(is);
                
                LightblueHttpClient client = new LightblueHttpClient(lbClientFilePath);
                
                for(Query query: Query.fromProperties(p)) {
                    for (int i = 0; i < query.getThreads(); i++) {
                        new Thread(new QueryRunner(query, client)).start();
                    }
                }                                
                
            } catch (IOException e) {
                log.error("Can read " + queriesFilePath, e);
                System.exit(1);
            }

        } catch (MissingOptionException e) {
            log.error(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(LightblueLoadGenerator.class.getSimpleName(), options);            
            System.exit(1);
        }
        
        

        
    }
}
