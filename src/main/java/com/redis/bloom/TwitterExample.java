package com.redis.bloom;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import io.rebloom.client.Client;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterExample {

  public static Properties readPropertiesFile(String fileName) throws IOException {
    InputStream fis = null;
    Properties prop = null;
    try {
      prop = new Properties();
      fis = TwitterExample.class.getClassLoader().getResourceAsStream(fileName);

      if (fis != null) {
        prop.load(fis);
      } else {
        throw new FileNotFoundException("property file '" + fileName + "' not found in the classpath");
      }
    } finally {
      fis.close();
    }

    return prop;
  }

  public static void main(String[] args) throws TwitterException, FileNotFoundException, IOException {
    Properties prop = readPropertiesFile("./creds.properties");
    String oaConsumerKey = prop.getProperty("OAuthConsumerKey");
    String oaConsumerSecret = prop.getProperty("OAuthConsumerSecret");
    String oaAccessToken = prop.getProperty("OAuthAccessToken");
    String oaAccessTokenSecret = prop.getProperty("OAuthAccessTokenSecret");

    // do categories of languages and frameworks...

    Client client = new Client("localhost", 6379);

    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setDebugEnabled(true).setOAuthConsumerKey(oaConsumerKey).setOAuthConsumerSecret(oaConsumerSecret)
        .setOAuthAccessToken(oaAccessToken).setOAuthAccessTokenSecret(oaAccessTokenSecret);

    FilterQuery query = new FilterQuery("tesla", "stock", "elon", "musk", "#sell", "#buy");

    client.delete("buyOrSell");
    client.cmsInitByDim("buyOrSell", 16L, 16L);

    new TwitterStreamFactory(cb.build()).getInstance().addListener(new StatusListener() {

      @Override
      public void onStatus(Status status) {
        System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
        long buyCount = StringUtils.countMatches(status.getText(), "buy");
        long sellCount = StringUtils.countMatches(status.getText(), "sell");
        System.out.println("Counts: BUY " + buyCount + ", SELL " + sellCount);
        client.cmsIncrBy("buyOrSell", Map.of("buy", buyCount));
        client.cmsIncrBy("buyOrSell", Map.of("sell", sellCount));
      }

      @Override
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
      }

      @Override
      public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
      }

      @Override
      public void onScrubGeo(long userId, long upToStatusId) {
        System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
      }

      @Override
      public void onStallWarning(StallWarning warning) {
        System.out.println("Got stall warning:" + warning);
      }

      @Override
      public void onException(Exception ex) {
        ex.printStackTrace();
      }
    }).filter(query);
  }

}
