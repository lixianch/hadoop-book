import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Created by lixianch on 2016/8/11.
 */
public class RegexExcludePathFilter implements PathFilter {
    private final String regex;
    public RegexExcludePathFilter(String regex){
        this.regex = regex;
    }
    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(regex);
    }
}
