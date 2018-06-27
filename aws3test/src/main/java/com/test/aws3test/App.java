package com.test.aws3test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import io.findify.s3mock.S3Mock;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        S3Mock api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
        
        try {
            api.start();
            
            AwsS3Utils.createBucket("mybucket", Region.US_West_2);
            System.out.println( AwsS3Utils.getAllBucket() );
            
            // upload data to aws s3
            AwsS3Utils.upload("mybucket", "myimage/test.png", new File("./test.png"));
            AwsS3Utils.upload("mybucket", "mydata/clickthru_43029.csv", new File("./test.csv"));
            
            // read data from bucket
            S3Object object = AwsS3Utils.getS3Object("mybucket", "mydata/clickthru_43029.csv");
            try(InputStream is = object.getObjectContent()){
                try(BufferedReader reader = new BufferedReader( new InputStreamReader(is))){
                    String line = null;
                    while((line = reader.readLine()) != null) {
                        System.out.println(line);
                    }   
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            
            
            
        } finally {
            AwsS3Utils.getAmazonS3().shutdown();
            api.stop();
        }
    }
}


class AwsS3Utils {
    
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AwsS3Utils.class);

    private static String accessKeyID = "accessKeyID";
    private static String secretKey = "secretKey";
    
    private static AmazonS3 s3Client = null;
    
    public static AmazonS3 getAmazonS3() {
        
        if(s3Client == null) {
            synchronized (AwsS3Utils.class) {
                try {
                    EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2");
                    
/*                    
                    AWSCredentials credentials = new BasicAWSCredentials(accessKeyID, secretKey);
                    s3Client = AmazonS3ClientBuilder.standard()
                            .withCredentials(new AWSStaticCredentialsProvider(credentials))
                            //.withRegion(Regions.US_WEST_2)
                            .withDualstackEnabled(true)
                            .build();
*/                    
                    s3Client = AmazonS3ClientBuilder
                            .standard()
                            .withPathStyleAccessEnabled(true)  
                            .withEndpointConfiguration(endpoint)
                            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))     
                            .build();
                }
                catch(AmazonServiceException e) {
                    // The call was transmitted successfully, but Amazon S3 couldn't process 
                    // it, so it returned an error response.
                    logger.error("Can not start aws s3: ",e);
                }
                catch(SdkClientException e) {
                    // Amazon S3 couldn't be contacted for a response, or the client
                    // couldn't parse the response from Amazon S3.
                    logger.error("Can not start aws s3: ",e);
                } 
            }
            
        }
        
        return s3Client;
    }
    
    public static Bucket createBucket(String bucketName, Region region) {
        Bucket bucket = null;
        if (!getAmazonS3().doesBucketExistV2(bucketName)) {
            bucket = getAmazonS3().createBucket(new CreateBucketRequest(bucketName, region));
        }
        return bucket;
    }
    
    public static void deleteBucket(String bucketName){
        getAmazonS3().deleteBucket(bucketName);
    }
    
    public static List<Bucket> getAllBucket(){
        return getAmazonS3().listBuckets();
    }
    
    public static PutObjectResult upload(String bucketName,String key,File file){
        return getAmazonS3().putObject(new PutObjectRequest(bucketName, key, file));
    }
    
    public static S3Object getS3Object(String bucketName,String key) {
        S3Object object = getAmazonS3().getObject(new GetObjectRequest(bucketName, key));
        
        return object;
    }
    
    public static void deleteObject(String bucketName,String key){
        getAmazonS3().deleteObject(bucketName, key);
    }
    
    public static void downloadFile(String bucketName,String key,String targetFilePath){
        S3Object object = getAmazonS3().getObject(new GetObjectRequest(bucketName,key));
        if(object!=null){
            System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
            InputStream input = null;
            FileOutputStream fileOutputStream = null;
            byte[] data = null;
            try {
                input=object.getObjectContent();
                data = new byte[input.available()];
                int len = 0;
                fileOutputStream = new FileOutputStream(targetFilePath);
                while ((len = input.read(data)) != -1) {
                    fileOutputStream.write(data, 0, len);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }finally{
                if(fileOutputStream!=null){
                    try {
                        fileOutputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if(input!=null){
                    try {
                        input.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
    
    
    
}