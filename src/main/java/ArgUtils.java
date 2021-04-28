import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ArgUtils {
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
}
