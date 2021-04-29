import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.ArrayList;
import java.util.StringTokenizer;

public class Utils {
    public static Options addOptions(Options options, String add) {
        if (add.contains("i")) {
            Option input = new Option("i", "input", true, "input folder");
            input.setRequired(true);
            options.addOption(input);
        }
        if (add.contains("o")) {
            Option output = new Option("o", "output", true, "output folder");
            output.setRequired(true);
            options.addOption(output);
        }
        if (add.contains("r")) {
            Option rmdir = new Option("r", "rmdir", true, "auto-remove output directory");
            options.addOption(rmdir);
        }
        return options;
    }

    public static ArrayList<String> readLineCsv(String line) {
        ArrayList<String> list = new ArrayList<>();
        StringTokenizer itr = new StringTokenizer(line, ",");
        while (itr.hasMoreTokens()) {
            list.add(itr.nextToken().trim());
        }
        return list;
    }

}
