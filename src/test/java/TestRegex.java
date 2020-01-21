import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegex {



    public static void main(String[] args){

        Pattern storeSizePattern = Pattern.compile("(([1-9]\\d*\\.?\\d+)|(0\\.\\d*[1-9])|(\\d+))(mb|kb|b)");
        Pattern dailyIndexPattern = Pattern.compile("\\w+((((19|20)\\d{2})_(0?[13-9]|1[012])_(0?[1-9]|[12]\\d|30))|(((19|20)\\d{2})_(0?[13578]|1[02])_31)|(((19|20)\\d{2})_0?2_(0?[1-9]|1\\d|2[0-8]))|((((19|20)([13579][26]|[2468][048]|0[48]))|(2000))_0?2_29))\\w+");

        String storeSize = "12.8";

        String index = "dns_2019_01_08_filteresmodel";

        Matcher storeMatcher = storeSizePattern.matcher(storeSize);

        Matcher indexMatcher = dailyIndexPattern.matcher(index);

        System.out.println(indexMatcher.matches());

    }


}
