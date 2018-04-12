package test.streaming.demo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import scala.Tuple2;
import scala.collection.JavaConversions;
import utils.KafkaOffsetTool;

public class WordCountFromKafkaDirect {
	
	public static void main(String[] args) {
		
		//SparkConf conf = new SparkConf().setAppName("WordCountFromKafka").setMaster("local[2]");
		SparkConf conf = new SparkConf().setAppName("WordCountFromKafka");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		//String zk="34.213.211.112:2181,54.203.117.18:2181,54.191.251.19:2181";
		String brokers="172.31.22.6:9092,172.31.27.71:9092";
		String topics=args[0];
		
		Set<String> topicsSet = new HashSet<>();
		topicsSet.add(topics);
		Map<String, String> kafkaParams = new HashMap<>();
	    //kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("bootstrap.servers", brokers);
		//kafkaParams.put("auto.commit.enable", "false");
	    //kafkaParams.put("zookeeper.connect", "34.208.23.204:2181,54.245.220.108:2181,34.209.142.234:2181");
	    //kafkaParams.put("auto.offset.reset", "latest");
	    kafkaParams.put("group.id", "direct1");
	    scala.collection.immutable.Map<String, String> scalaKafkaParams=new scala.collection.immutable.HashMap<String, String>();
	    scalaKafkaParams=scalaKafkaParams.$plus(new Tuple2<String, String>("bootstrap.servers", kafkaParams.get("bootstrap.servers")));
	    scalaKafkaParams=scalaKafkaParams.$plus(new Tuple2<String, String>("group.id", kafkaParams.get("group.id")));
	    System.out.println(scalaKafkaParams);
//	    scala.collection.mutable.Map<String, String> testMap = JavaConversions.mapAsScalaMap(kafkaParam);
//        scala.collection.immutable.Map<String, String> scalaKafkaParam =
//                testMap.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
//					private static final long serialVersionUID = 1L;
//					public Tuple2<String, String> apply(Tuple2<String, String> v1) {
//                        return v1;
//                    }
//                });
	    KafkaCluster kafkaCluster = new KafkaCluster(scalaKafkaParams);
        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicsSet);
        scala.collection.immutable.Set<String> immutableTopics = mutableTopics.toSet();
        scala.collection.immutable.Set<TopicAndPartition> sourceTopicAndPartitionSet = kafkaCluster.getPartitions(immutableTopics).right().get();
     
        // kafka direct stream 初始化时使用的offset数据
        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap<TopicAndPartition, Long>();
	 // 没有保存offset时（该group首次消费时）
		if (kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id"), sourceTopicAndPartitionSet).isLeft()) {
			consumerOffsetsLong = KafkaOffsetTool.getInstance().getEarliestOffset(brokers,
			Arrays.asList(new String[] { topics }), kafkaParams.get("group.id"));
			System.out.println(consumerOffsetsLong);
		} else { // offset已存在, 使用保存的offset
			scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = kafkaCluster
					.getConsumerOffsets(kafkaParams.get("group.id"), sourceTopicAndPartitionSet).right().get();
			Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);
			Set<TopicAndPartition> set = consumerOffsets.keySet();
			for (TopicAndPartition topicAndPartition : set) {
				Long offset = (Long) consumerOffsets.get(topicAndPartition);
				consumerOffsetsLong.put(topicAndPartition, offset);
			}
		}
	    
//		JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(jssc, String.class,
//				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		JavaInputDStream<String> directStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParams,
                consumerOffsetsLong,
                new Function<MessageAndMetadata<String, String>, String>() {
					private static final long serialVersionUID = 1L;

					public String call(MessageAndMetadata<String, String> v1) throws Exception {
                        return v1.message();
                    }
                }
        );
		
		
		directStream.foreachRDD(rdd -> {
			  OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			  for(OffsetRange o:offsetRanges){
				  System.out.println(
					      "hello "+o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
			  }
			  rdd.foreachPartition(consumerRecords -> {
				  
				consumerRecords.forEachRemaining(consumerRecord -> {
					System.out.println(consumerRecord);
				});
			    OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
			    System.out.println(
			      o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
			    
			    //offsetRanges.set(offsetRanges);
			    //((CanCommitOffsets) directStream.inputDStream()).commitAsync(offsetRanges);
			    //directStream.inputDStream().
			  });
			});
		
		
//		JavaDStream<String> words = directStream.flatMap(line->Arrays.asList(line._2.split(" ")));
//		JavaPairDStream<String, Integer> wordCount = words.mapToPair(word->new Tuple2<String, Integer>(word,1));
//		JavaPairDStream<String, Integer> results = wordCount.reduceByKey((c1,c2)->addInt(c1,c2));
//		results.print();
		jssc.start();// Start the computation
		jssc.awaitTermination();
	}
	
	private static int addInt(int c1,int c2) {
		return c1+c2;
	}
   

}
