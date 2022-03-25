/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.gael.nifi.processors.StreamS3Object;

import static org.apache.nifi.processors.aws.s3.PutS3Object.CONTENT_TYPE;
import static org.apache.nifi.processors.aws.s3.PutS3Object.MULTIPART_PART_SIZE;
import static org.apache.nifi.processors.aws.s3.PutS3Object.MULTIPART_THRESHOLD;
import static org.apache.nifi.processors.aws.s3.PutS3Object.STORAGE_CLASS;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.processors.aws.s3.AbstractS3Processor;
import org.apache.nifi.processors.aws.s3.ListS3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;;

/**
 * Processor that copy objects from one S3 bucket to an other without making a local copy.
 *
 * @author Patrick Pradier
 */
@Tags({ "Amazon", "S3", "AWS", "Archive", "Put", "Stream" })
@CapabilityDescription("Copy objects from one S3 bucket to an other.")
@SeeAlso({ ListS3.class })
@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes({ @ReadsAttribute(attribute = "filename", description = "Filename of the source object"),
      @ReadsAttribute(attribute = "s3.bucket", description = "Bucket of the source object") })
@WritesAttributes({ @WritesAttribute(attribute = "filename", description = "Filename of the copied object") })
public class StreamS3ObjectProcessor extends AbstractS3Processor
{
   /* Relationships : success and failure */
   private Set<Relationship> relationships = new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE));

   /* Destination credentials */
   public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE =
      new PropertyDescriptor.Builder().name("AWS Credentials Provider service")
            .description("The Controller Service that is used to obtain aws credentials provider").required(false)
            .identifiesControllerService(AWSCredentialsProviderService.class).build();

   /* Source credentials */
   public static final PropertyDescriptor SOURCE_AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
         .name("Source AWS Credentials Provider service").displayName("Source AWS Credentials Provider service")
         .description("The Controller Service that is used to obtain aws credentials provider from the source storage")
         .required(false).identifiesControllerService(AWSCredentialsProviderService.class).build();

   /* Source region */
   public static final PropertyDescriptor SOURCE_REGION = new PropertyDescriptor.Builder().name("sourceRegion")
         .displayName("Source Region").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
         .description("The source region of the objects.").build();

   /* Source endpoint */
   public static final PropertyDescriptor SOURCE_ENDPOINT_OVERRIDE =
      new PropertyDescriptor.Builder().name("sourceEndpoint").displayName("Source Endpoint Override").required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The source endpoint if objects are stored in a non AWS cloud").build();

   /* Source access key */
   public static final PropertyDescriptor SOURCE_ACCESS_KEY = new PropertyDescriptor.Builder().name("sourceAccessKey")
         .displayName("Source Access Key").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
         .sensitive(true).description("Source access key.").build();

   /* Source secret key */
   public static final PropertyDescriptor SOURCE_SECRET_KEY = new PropertyDescriptor.Builder().name("sourceSecretKey")
         .displayName("Source Secret Key").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
         .sensitive(true).description("Source secret key.").build();

   /* Dest access key */
   public static final PropertyDescriptor ACCESS_KEY =
      new PropertyDescriptor.Builder().name("accessKey").displayName("Access Key").required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(true).description("Access key.").build();

   /* Dest secret key */
   public static final PropertyDescriptor SECRET_KEY =
      new PropertyDescriptor.Builder().name("secretKey").displayName("Secret Key").required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(true).description("secret key.").build();

   /* Dest region */
   public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder().name("region").displayName("Region")
         .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).description("Region").build();

   /* Max connection per AmazonS3 client */
   public static final PropertyDescriptor MAX_CONNECTIONS = new PropertyDescriptor.Builder().name("maxConnections")
         .displayName("Max connections").required(false).addValidator(StandardValidators.INTEGER_VALIDATOR)
         .defaultValue("200").description("Max connections per AmazonS3 client").build();

   /* Max retries */
   public static final PropertyDescriptor MAX_RETRIES = new PropertyDescriptor.Builder().name("maxRetries")
         .displayName("Max retries").required(false).addValidator(StandardValidators.INTEGER_VALIDATOR)
         .defaultValue("3").description("Max retries per copy").build();

   /* Max connection per AmazonS3 client */
   public static final PropertyDescriptor MULTIPART_THRESHOLD =
      new PropertyDescriptor.Builder().name("multipartTreshold").displayName("Multipart Treshhold (MB)").required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR).defaultValue("5000")
            .description("Multipart Treshhold (MB)").build();

   /* Max connection per AmazonS3 client */
   public static final PropertyDescriptor MULTIPART_PART_SIZE = new PropertyDescriptor.Builder().name("multipartSize")
         .displayName("Multipart Size (MB)").required(false).addValidator(StandardValidators.INTEGER_VALIDATOR)
         .defaultValue("5000").description("Multipart Size (MB)").build();

   public static final List<PropertyDescriptor> properties =
      Collections.unmodifiableList(Arrays.asList(SOURCE_AWS_CREDENTIALS_PROVIDER_SERVICE, SOURCE_ACCESS_KEY,
            SOURCE_SECRET_KEY, SOURCE_ENDPOINT_OVERRIDE, SOURCE_REGION, BUCKET, CONTENT_TYPE,
            AWS_CREDENTIALS_PROVIDER_SERVICE, ACCESS_KEY, SECRET_KEY, REGION, ENDPOINT_OVERRIDE, STORAGE_CLASS,
            MULTIPART_THRESHOLD, MULTIPART_PART_SIZE, MAX_CONNECTIONS, MAX_RETRIES));

   /* Only one instance of those objects is needed */
   private AmazonS3 sourceClient;
   private AmazonS3 destClient;
   private TransferManager multipartManager;
   private TransferManager singleManager;

   @Override
   public final List<PropertyDescriptor> getSupportedPropertyDescriptors()
   {
      return properties;
   }

   @Override
   public Set<Relationship> getRelationships()
   {
      return this.relationships;
   }

   @Override
   public void onScheduled(ProcessContext context)
   {
      getSourceClient(context);
      getDestClient(context);
      getSingleTransferManager(context, destClient);
      getMultipartTransferManager(context, destClient);

      // create bucket
      String destBucket = context.getProperty(BUCKET).getValue();
      if (!destClient.doesBucketExistV2(destBucket))
      {
         destClient.createBucket(destBucket);
         getLogger().info("Bucket {} created in destination", destBucket);
      }
   }

   @Override
   public void onShutDown()
   {
      if (singleManager != null)
      {
         singleManager.shutdownNow();
      }
      if (multipartManager != null)
      {
         multipartManager.shutdownNow();
      }
      if (sourceClient != null)
      {
         sourceClient.shutdown();
      }
      if (destClient != null)
      {
         destClient.shutdown();
      }

   }

   @Override
   public void onTrigger(ProcessContext context, ProcessSession session)
   {
      FlowFile flowFile = session.get();
      if (flowFile == null)
      {
         return;
      }

      String sourceBucket = flowFile.getAttribute("s3.bucket");
      String filename = flowFile.getAttribute("filename");
      String destBucket = context.getProperty(BUCKET).getValue();

      ObjectMetadata srcmetadata = getSourceClient(context).getObjectMetadata(sourceBucket, filename);
      ObjectMetadata objectMetadata = new ObjectMetadata();
      long contentLength = srcmetadata.getContentLength();
      objectMetadata.setContentLength(contentLength);

      TransferManager transferManager = chooseTransferManager(context, getDestClient(context), contentLength);

      try (InputStream source =
         new BufferedInputStream(getSourceClient(context).getObject(sourceBucket, filename).getObjectContent()))
      {

         Upload upload = transferManager.upload(destBucket, filename, source, srcmetadata);
         upload.waitForCompletion();
         getLogger().info("Copy successful for object {}", flowFile);
         session.transfer(flowFile, REL_SUCCESS);
      }
      catch (AmazonClientException | InterruptedException | IOException e)
      {
         getLogger().error("Failed to copy {} to Amazon S3 due to {}", flowFile, e);
         flowFile = session.penalize(flowFile);
         session.transfer(flowFile, REL_FAILURE);
      }

   }

   private synchronized TransferManager chooseTransferManager(ProcessContext context, AmazonS3 client,
         long contentLength)
   {
      if (contentLength < context.getProperty(MULTIPART_THRESHOLD).asLong() * 1000000)
      {
         return getSingleTransferManager(context, client);
      }
      return getMultipartTransferManager(context, client);
   }

   private synchronized TransferManager getMultipartTransferManager(ProcessContext context, AmazonS3 client)
   {
      if (multipartManager == null)
      {
         multipartManager = TransferManagerBuilder.standard().withS3Client(client)
               .withMultipartCopyThreshold(context.getProperty(MULTIPART_THRESHOLD).asLong() * 1000000)
               .withMultipartCopyPartSize(context.getProperty(MULTIPART_PART_SIZE).asLong() * 1000000)
               .withMultipartUploadThreshold(context.getProperty(MULTIPART_THRESHOLD).asLong() * 1000000)
               .withMinimumUploadPartSize(context.getProperty(MULTIPART_PART_SIZE).asLong() * 1000000).build();
      }
      return multipartManager;
   }

   private synchronized TransferManager getSingleTransferManager(ProcessContext context, AmazonS3 client)
   {
      if (singleManager == null)
      {
         singleManager = TransferManagerBuilder.standard().withS3Client(client).build();
      }
      return singleManager;
   }

   private synchronized AmazonS3 getSourceClient(ProcessContext context)
   {
      if (sourceClient == null)
      {
         AWSCredentialsProviderService service = context.getProperty(SOURCE_AWS_CREDENTIALS_PROVIDER_SERVICE)
               .asControllerService(AWSCredentialsProviderService.class);
         String endpoint = context.getProperty(SOURCE_ENDPOINT_OVERRIDE).getValue();
         String region = context.getProperty(SOURCE_REGION).getValue();
         int maxConnections = context.getProperty(MAX_CONNECTIONS).asInteger();
         int maxRetries = context.getProperty(MAX_RETRIES).asInteger();
         String accessKey = context.getProperty(SOURCE_ACCESS_KEY).getValue();
         String secretKey = context.getProperty(SOURCE_SECRET_KEY).getValue();
         if (service != null)
         {
            sourceClient = getAmazonClient(service, endpoint, region, maxConnections, maxRetries);
         }
         else
         {
            sourceClient = getAmazonClient(accessKey, secretKey, endpoint, region, maxConnections, maxRetries);
         }
      }
      return sourceClient;
   }

   private synchronized AmazonS3 getDestClient(ProcessContext context)
   {
      if (destClient == null)
      {
         AWSCredentialsProviderService service = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE)
               .asControllerService(AWSCredentialsProviderService.class);
         String endpoint = context.getProperty(ENDPOINT_OVERRIDE).getValue();
         String region = context.getProperty(REGION).getValue();
         String accessKey = context.getProperty(ACCESS_KEY).getValue();
         String secretKey = context.getProperty(SECRET_KEY).getValue();
         int maxConnections = context.getProperty(MAX_CONNECTIONS).asInteger();
         int maxRetries = context.getProperty(MAX_RETRIES).asInteger();
         if (service != null)
         {
            destClient = getAmazonClient(service, endpoint, region, maxConnections, maxRetries);
         }
         else
         {
            destClient = getAmazonClient(accessKey, secretKey, endpoint, region, maxConnections, maxRetries);
         }
      }
      return destClient;
   }

   private AmazonS3 getAmazonClient(AWSCredentialsProviderService service, String endpoint, String region,
         int maxConnections, int maxRetries)
   {
      /* Custom endpoint */
      if (endpoint != null && !endpoint.isEmpty())
      {
         return AmazonS3ClientBuilder.standard()
               .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
               .withClientConfiguration(
                     new ClientConfiguration().withMaxConnections(maxConnections).withMaxErrorRetry(maxRetries))
               .withCredentials(service.getCredentialsProvider()).build();
      }
      /* AWS endpoint */
      else
      {
         return AmazonS3ClientBuilder.standard()
               .withClientConfiguration(
                     new ClientConfiguration().withMaxConnections(maxConnections).withMaxErrorRetry(maxConnections))
               .withCredentials(service.getCredentialsProvider()).withRegion(region).build();
      }
   }

   private AmazonS3 getAmazonClient(String accessKey, String secretKey, String endpoint, String region,
         int maxConnections, int maxRetries)
   {
      /* Custom endpoint */
      if (endpoint != null && !endpoint.isEmpty())
      {
         if (accessKey != null && !accessKey.isEmpty())
         {
            return AmazonS3ClientBuilder.standard()
                  .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                  .withClientConfiguration(
                        new ClientConfiguration().withMaxConnections(maxConnections).withMaxErrorRetry(maxRetries))
                  .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                  .build();
         }
         else
         {
            return AmazonS3ClientBuilder.standard()
                  .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                  .withClientConfiguration(
                        new ClientConfiguration().withMaxConnections(maxConnections).withMaxErrorRetry(maxRetries))
                  .build();
         }
      }
      /* AWS endpoint */
      else
      {
         if (accessKey != null && !accessKey.isEmpty())
         {
            return AmazonS3ClientBuilder.standard().withRegion(region)
                  .withClientConfiguration(
                        new ClientConfiguration().withMaxConnections(maxConnections).withMaxErrorRetry(maxConnections))
                  .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                  .build();
         }
         else
         {
            return AmazonS3ClientBuilder.standard().withRegion(region)
                  .withClientConfiguration(
                        new ClientConfiguration().withMaxConnections(maxConnections).withMaxErrorRetry(maxConnections))
                  .build();
         }
      }
   }
}
