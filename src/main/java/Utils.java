import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

public class Utils {

    public static String[] readTwitterCredentials() throws IOException {

        String file ="src/main/resources/credentials.txt";

        String[] credentials = new String[4];

        BufferedReader reader = new BufferedReader(new FileReader(file));
        credentials[0] = reader.readLine();
        credentials[1] = reader.readLine();
        credentials[2] = reader.readLine();
        credentials[3] = reader.readLine();
        reader.close();

        return credentials;

    }

}
