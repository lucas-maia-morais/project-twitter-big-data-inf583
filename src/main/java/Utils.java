import org.apache.commons.math3.distribution.ChiSquaredDistribution;

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

    /**
     * @param poissonMean estimated parameter of the Poisson distribution
     * @param nSamples number of samples used for the estimation (don't be afraid if this is 1 ^^')
     * @param alpha confidence level
     * @return the approx. length of the alpha-confidence interval for a Poisson distribution of sample mean poissonMean
     */
    public static double CI(double poissonMean, int nSamples, double alpha) {

        // estimate the confidence interval for nSamples * mean using nSamples * sample mean
        // (as in https://en.wikipedia.org/wiki/Poisson_distribution#Confidence_interval)
        double k = Math.max(nSamples * poissonMean, 1); // PS. ChiSquared does not accept param zero
        ChiSquaredDistribution lowerChiSquared = new ChiSquaredDistribution(2 * k);
        ChiSquaredDistribution upperChiSquared = new ChiSquaredDistribution(2 * k + 2);

        return upperChiSquared.inverseCumulativeProbability(1-alpha/2) / 2
                - lowerChiSquared.inverseCumulativeProbability(alpha/2) / 2;

    }

}
