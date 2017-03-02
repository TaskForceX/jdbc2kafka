package io.taskforcex.tool;

import org.apache.commons.cli.*;

import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by dgt on 3/1/17.
 */
public class JdbcToKafkaTool {

    private static final Option HELP_OPT = new Option("h", "help", false, "print help message");
    private static final Option CONFIG_FILE_OPT = new Option("c", "config", true, "config file");

    public static void main(String[] args) throws FileNotFoundException, SQLException {
        new JdbcToKafkaTool().run(args);
    }

    private Options getOptions() {
        Options options = new Options();
        options.addOption(HELP_OPT);
        options.addOption(CONFIG_FILE_OPT);

        return options;
    }

    private CommandLine parseOptions(String[] args) {
        Options options = getOptions();
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine commandLine = null;

        try {
            commandLine = commandLineParser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (commandLine.hasOption(HELP_OPT.getOpt())) {
            printHelpAndExit(options, 0);
        }

        if (!commandLine.hasOption(CONFIG_FILE_OPT.getOpt())) {
            printHelpAndExit("Missing command line options!", options);
        }

        return commandLine;
    }

    private void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        printHelpAndExit(options, 1);

    }

    private void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("java -cp jdbc2kafka-1.0.0-jar-with-dependencies.jar io.taskforcex.tool.JdbcToKafkaTool", options);
        System.exit(exitCode);
    }

    private void run(String[] args) throws FileNotFoundException {
        CommandLine commandLine = parseOptions(args);
        Configuration configuration = new Configuration();
        configuration.addYamlResource(commandLine.getOptionValue(CONFIG_FILE_OPT.getOpt()));

        try (
                JdbcSource jdbcSource = new JdbcSource(
                        configuration.getString(JdbcToKafkaToolConfiguration.JDBC_URL_CONFIG),
                        configuration.getString(JdbcToKafkaToolConfiguration.JDBC_USER_CONFIG),
                        configuration.getString(JdbcToKafkaToolConfiguration.JDBC_PASSWORD_CONFIG));
                Statement statement = jdbcSource.getConnection().createStatement();
                ResultSet resultSet = statement.executeQuery(
                        configuration.getString(JdbcToKafkaToolConfiguration.JDBC_SQL_CONFIG));
                IPublisher kafkaPublisher = new KafkaPublisher(
                        configuration.getString(JdbcToKafkaToolConfiguration.KAFKA_BOOTSTRAP_SERVERS_CONFIG));

        ) {
            while (resultSet.next()) {
                kafkaPublisher.pubSync(
                        configuration.getString(JdbcToKafkaToolConfiguration.KAFKA_TOPIC_CONFIG),
                        null,
                        JdbcSource.resultToJson(resultSet).toString());
            }

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        System.exit(0);
    }
}
