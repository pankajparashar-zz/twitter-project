package com.akesireddy.twitterfeeds;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterFeeds {

public static void main(String[] args) throws TwitterException, IOException, InterruptedException {
// TODO Auto-generated method stub
String hashTag="ONS";
TwitterFeeds.hashTagSearch(hashTag);
//TwitterFeeds.getTimeLine();
}

static Twitter getTwitterObj(){
ConfigurationBuilder cb = new ConfigurationBuilder();
cb.setDebugEnabled(true)
.setOAuthConsumerKey(“XXX")
.setOAuthConsumerSecret(“XXX")
.setOAuthAccessToken(“XXX")
.setOAuthAccessTokenSecret(“XXX");
TwitterFactory tf = new TwitterFactory(cb.build());
return tf.getInstance();
}

static void hashTagSearch(String hashTag) throws TwitterException, IOException, InterruptedException{
Twitter twitter = getTwitterObj();
int count=1;
        try {
            Query query = new Query(hashTag);
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                (new SaveObject(tweets,count)).save();
                count++;
                Thread.sleep(5000);
            } while ((query = result.nextQuery()) != null);
            System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }
}
static void getTimeLine() throws TwitterException, IOException, InterruptedException{
Twitter twitter = getTwitterObj();
Thread.sleep(5000);
System.exit(0);
}
}

@SuppressWarnings("serial")
class SaveObject implements Serializable {
List<Status> statuses=null;
int count=0;
public SaveObject(List<Status> statuses,int count){
this.statuses=statuses;
this.count=count;
}
@SuppressWarnings("resource")
void save() throws IOException{
FileOutputStream fo = new FileOutputStream(this.count+".data");
ObjectOutputStream obj_out = new ObjectOutputStream (fo);
obj_out.writeObject (this.statuses);
}
}