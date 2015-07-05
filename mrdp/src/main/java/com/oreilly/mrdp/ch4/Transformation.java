package com.oreilly.mrdp.ch4;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

public class Transformation extends Configured implements Tool {

	public static class XmlToJsonMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private Text outkey = new Text();
		private NullWritable outvalue = NullWritable.get();
		private DocumentBuilder bldr = null;
		private ObjectNode json = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// Create the JSON object that we will use to store the data fields
			ObjectMapper mapper = new ObjectMapper();
			json = mapper.createObjectNode();

			try {
				// Create the XML document builder to parse the XML string
				DocumentBuilderFactory factory = DocumentBuilderFactory
						.newInstance();
				bldr = factory.newDocumentBuilder();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// Validate input is a "row" XML record
			if (!value.toString().trim().startsWith("<row")) {
				return;
			}

			Document doc;
			try {
				// Parse the XML into a Document
				doc = bldr.parse(new ByteArrayInputStream(value.toString()
						.getBytes()));
			} catch (SAXException e) {
				context.getCounter("Exceptions", "SAX").increment(1);
				return;
			}

			// Clear all data from the node, re-using the same object
			json.removeAll();
			// Get the attribute map from the document element
			NamedNodeMap attributes = doc.getDocumentElement().getAttributes();

			// For each attribute
			for (int i = 0; i < attributes.getLength(); ++i) {

				// Get the node and validate it is an attribute node
				Node node = attributes.item(i);
				if (node.getNodeType() == Node.ATTRIBUTE_NODE) {
					// Add a JSON field with the XML attribute as the field
					// name and the attribute value as the field value
					json.put(node.getNodeName(), node.getNodeValue());
				}
			}
			// Set the output key to the JSON string and write to the
			// context
			outkey.set(json.toString());
			context.write(outkey, outvalue);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Transformation <xml> <json-output>");
			return 1;
		}

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		// Configure job to prepare for sampling
		Job job = Job.getInstance(getConf(), "Transformation");
		job.setJarByClass(Transformation.class);

		// Use the mapper implementation with zero reduce tasks
		job.setMapperClass(XmlToJsonMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		TextInputFormat.setInputPaths(job, input);

		// Set the output format to a text file
		TextOutputFormat.setOutputPath(job, output);

		// Submit the job and exit
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new Transformation(),
				args));
	}
}
