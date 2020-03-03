/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fintechstudios;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.time.ZoneId;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// enable checkpointing
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointInterval(1000); // every second
		checkpointConfig.setCheckpointTimeout(10000); // 10 seconds
		checkpointConfig.setMinPauseBetweenCheckpoints(500); // half a second
		checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

		DataStream<Record> recordStream = env
//				.addSource(new BoundedIncrementingRecordSource(1000))
//				.name("Incrementing record source")
//				.uid("source")

				.addSource(new UnboundedIncrementingRecordSource(500))
				.name("Unbounded Incrementing record source")
				.uid("unbounded-source")
				;

		// cwd
		String basePath = FileSystems.getDefault().getPath(".").toAbsolutePath().toString();
		System.out.println("basePath = " + basePath);
		// assign buckets by seconds
		DateTimeBucketAssigner<Record> bucketAssigner = new DateTimeBucketAssigner<>("ss", ZoneId.of("UTC"));

		StreamingFileSink<Record> filSink = StreamingFileSink
				.forBulkFormat(new Path(basePath + "/text"),
						new BulkWriter.Factory<Record>() {
							@Override
							public BulkWriter<Record> create(FSDataOutputStream out) throws IOException {
								final TarArchiveOutputStream compressedOutputStream = new TarArchiveOutputStream(new GzipCompressorOutputStream(out));

								return new BulkWriter<Record>() {
									@Override
									public void addElement(Record record) throws IOException {
										TarArchiveEntry fileEntry = new TarArchiveEntry(String.format("%s.txt", record.getId()));
										byte[] fullTextData = "hey\nyou\nplease\nwork".getBytes(StandardCharsets.UTF_8);
										fileEntry.setSize(fullTextData.length);
										compressedOutputStream.putArchiveEntry(fileEntry);
										compressedOutputStream.write(fullTextData, 0, fullTextData.length);

										compressedOutputStream.closeArchiveEntry();
									}

									@Override
									public void flush() throws IOException {
										compressedOutputStream.flush();
									}

									@Override
									public void finish() throws IOException {
										this.flush();
									}
								};
							}
						})
				.withBucketAssigner(bucketAssigner)
				.withBucketCheckInterval(500)
				.build();

		recordStream
				.addSink(filSink)
				.name("Streaming File S3 Text Sink")
				.uid("streaming-file-s3-text-sink");

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
