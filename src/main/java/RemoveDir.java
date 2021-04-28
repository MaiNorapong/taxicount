import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RemoveDir {
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            formatter.printHelp("RemoveDir <file>...", "", options, "\n", true);
            System.out.println(e.getMessage());
            System.exit(1);
        }

        for (String a : cmd.getArgs()) {
            Path file = new Path(a);
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            fs.delete(file, true);
        }
    }
}
